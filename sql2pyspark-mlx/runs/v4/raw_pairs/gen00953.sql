with flattened as (
    select
        id,
        created_at,
        explode_outer(coupon_codes) as element
    from carts
),
valid as (
    select
        *,
        substring(element, 1, 4) as element_prefix,
        coalesce(element, 'NONE') as element_filled
    from flattened
    where created_at > to_date('2025-03-01')
      and to_date(created_at) >= '2025-10-25'
),
filtered as (
    select
        id,
        created_at,
        element,
        element_prefix,
        element_filled,
        cast(created_at as date) as created_at_day,
        datediff(current_date(), created_at) as days_since_created_at
    from valid
    where created_at >= timestamp '2025-03-01 18:00:00'
),
enriched as (
    select
        *,
        dense_rank() over (partition by element order by days_since_created_at desc nulls last) as days_since_created_at_dense_rank,
        dense_rank() over (partition by element order by days_since_created_at nulls first) as days_since_created_at_dense_rank_2,
        percent_rank() over (partition by element order by days_since_created_at desc nulls last) as days_since_created_at_percent_rank,
        avg(days_since_created_at) over (partition by element order by cast(created_at as bigint) range between 7200 preceding and current row) as days_since_created_at_avg_2h
    from filtered
),
scored as (
    select
        *,
        min(days_since_created_at_percent_rank) over (partition by element) as element_min_days_since_created_at_percent_rank,
        avg(days_since_created_at_dense_rank_2) over (partition by element) as element_avg_days_since_created_at_dense_rank_2,
        count(days_since_created_at) over (partition by element order by cast(created_at as bigint) range between 900 preceding and current row) as days_since_created_at_count_15m
    from enriched
)
select
    case when days_since_created_at_dense_rank_2 >= 8 then 'LARGE' else 'SMALL' end as days_since_created_at_dense_rank_2_tier,
    element,
    date_trunc('hour', created_at) as hour_window,
    count(*) filter (where element = 'SPRING10') as spring10_rows,
    min(days_since_created_at_dense_rank_2) as min_days_since_created_at_dense_rank_2,
    count(case when element = 'SPRING10' then 1 end) as spring10_count,
    count(*) filter (where element = 'WELCOME5') as welcome5_rows,
    percent_rank() over (partition by case when days_since_created_at_dense_rank_2 >= 8 then 'LARGE' else 'SMALL' end order by count(case when element = 'SPRING10' then 1 end) desc) as spring10_count_percent_rank
from scored
group by case when days_since_created_at_dense_rank_2 >= 8 then 'LARGE' else 'SMALL' end, element, date_trunc('hour', created_at);
