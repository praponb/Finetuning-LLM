with joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.end_date,
        src.monthly_fee,
        p.city,
        p.name
    from subscriptions src
    left join customers p
        on src.customer_id = p.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by customer_id, city order by end_date desc, id) as dedup_rank
    from joined
    qualify dedup_rank = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        year(end_date) as end_date_year,
        coalesce(city, 'NONE') as city_clean
    from ranked
    where end_date >= date '2023-10-02'
      and city is distinct from 'Bangkok'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        city,
        name,
        dedup_rank,
        avg(monthly_fee) over (partition by date_trunc('WEEK', end_date), customer_id order by monthly_fee, id rows between 2 preceding and 1 preceding) as monthly_fee_avg_2_prior,
        lag(monthly_fee, 1, 0) over (partition by customer_id order by monthly_fee, id) as prev_monthly_fee,
        avg(monthly_fee) over (partition by customer_id order by end_date, id rows between 3 preceding and current row) as monthly_fee_avg_3
    from filtered
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city order by dedup_rank desc, id) as row_num
    from with_windows
    qualify row_num = 1
)
select
    id,
    city,
    name,
    dedup_rank,
    monthly_fee_avg_2_prior,
    prev_monthly_fee,
    monthly_fee_avg_3,
    row_num,
    case when name rlike '^(Grace Kim|  Carol Diaz)$' then 'EXTENDED' else 'PRIORITY' end as name_zone
from deduped
order by city desc
