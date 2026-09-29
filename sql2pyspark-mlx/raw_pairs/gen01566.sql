with ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by country order by rating desc nulls first, id) as latest_rank
    from suppliers
    qualify latest_rank = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        max(rating) over w_ordered as last3_max_rating,
        avg(rating) over w_ordered as last3_avg_rating,
        sum(rating) over w_ordered as last3_sum_rating,
        min(rating) over w_ordered as last3_min_rating,
        sum(rating) over w_grp as running_rating
    from ranked
    where city in ('Bangkok', 'London', 'Singapore')
    window w_ordered as (partition by city order by rating, id rows between 2 preceding and current row), w_grp as (partition by country order by rating, id rows between unbounded preceding and current row)
),
kept_keys as (
    -- Set intersection on keys
    select country, city
    from metrics
    intersect
    select country, city
    from suppliers
    where length(supplier_name) < 10
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join kept_keys k on s.country = k.country and s.city = k.city
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        country,
        latest_rank,
        last3_max_rating,
        last3_sum_rating,
        last3_min_rating,
        dense_rank() over (partition by city order by last3_sum_rating nulls first) as last3_sum_rating_dense_rank,
        percent_rank() over (order by last3_avg_rating desc) as last3_avg_rating_percent_rank,
        round(running_rating / sum(running_rating) over (partition by city), 4) as running_rating_share,
        rank() over (order by running_rating desc nulls last) as running_rating_rank
    from reconciled
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.stock_qty > 134
        )
)
-- Grouped metrics
select
    country,
    max(last3_sum_rating) as max_last3_sum_rating,
    min(running_rating_share) as min_running_rating_share,
    count(case when country = 'TH' then 1 end) as th_count,
    grouping(country) as gid
from kept
group by cube (country);
