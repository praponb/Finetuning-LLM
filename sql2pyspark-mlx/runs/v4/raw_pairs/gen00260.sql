with metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(rating) over (partition by city order by rating, id rows between 2 preceding and current row) as rating_avg_2,
        row_number() over (order by rating desc, id) as event_seq,
        round(rating / sum(rating) over (partition by country), 4) as rating_share,
        ntile(10) over (partition by country, city order by rating desc, id) as rating_bucket,
        rank() over (partition by city order by rating nulls first) as rating_rank
    from suppliers
),
metrics_suppliers as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_avg_2,
        event_seq,
        rating_share,
        rating_bucket,
        rating_rank,
        dense_rank() over (order by rating desc) as rating_dense_rank,
        cume_dist() over (partition by city order by rating desc nulls last) as rating_cume_dist
    from metrics
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.cost,
        x.launch_date
    from metrics_suppliers s
    left join lateral (
            select c.cost, c.launch_date
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
),
remaining_keys as (
    -- Set difference on keys
    select id, city
    from latest_match
    except all
    select id, city
    from suppliers
    where rating < 2.5
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest_match s
    join remaining_keys k on s.id = k.id and s.city = k.city
)
-- Grouped metrics
select
    city,
    case when rating_rank >= 8 then 'LARGE' else 'SMALL' end as rating_rank_tier,
    count(*) filter (where country = 'JP') as jp_rows,
    size(collect_set(country)) as num_countries,
    count(case when city = 'New York' then 1 end) as new_york_count,
    round(sum(rating_bucket) / nullif(count(case when country = 'SG' then 1 end), 0), 2) as rating_bucket_per_sg,
    percentile_approx(event_seq, 0.5) as event_seq_p50,
    count(*) filter (where city = 'Bangkok') as bangkok_rows
from kept_rows
group by city, rating_rank_tier
order by bangkok_rows
