with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from suppliers s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.price > 740
        )
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.launch_date,
        x.cost
    from eligible s
    join lateral (
            select c.launch_date, c.cost
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.launch_date <= s.launch_date
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        rating,
        launch_date,
        cost,
        sum(rating) over (partition by date_trunc('YEAR', launch_date), city) as launch_date_period_sum_rating,
        row_number() over (partition by city, country order by launch_date desc, id) as row_seq,
        row_number() over (partition by country order by launch_date desc, id) as event_seq
    from not_flagged
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over w_grp as rating_bucket,
        ntile(5) over (order by cost desc, id) as cost_bucket,
        rank() over (order by rating desc) as rating_rank,
        ntile(5) over w_grp as rating_bucket_2,
        first_value(launch_date_period_sum_rating, true) over (order by cost, id rows between unbounded preceding and current row) as first_known_launch_date_period_sum_rating
    from enriched
    window w_grp as (order by rating, id)
)
-- Multi-level aggregation
select
    case when rating_rank >= 9 then 'LARGE' else 'SMALL' end as rating_rank_tier,
    date_trunc('month', launch_date) as activity_month,
    percentile_approx(rating, array(0.25, 0.5, 0.75)) as rating_quartiles,
    stddev_samp(rating) as rating_stddev
from with_windows
group by case when rating_rank >= 9 then 'LARGE' else 'SMALL' end, date_trunc('month', launch_date)
