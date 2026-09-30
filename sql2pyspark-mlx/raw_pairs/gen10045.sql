with cleaned as (
    -- Basic cleansing and defaults
    select *, concat(status, ' - ', plan) status_plan
    from subscriptions
    where end_date >= to_date('2023-11-03')
      and customer_id is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        end_date,
        percent_rank() over (partition by plan order by monthly_fee desc) monthly_fee_percent_rank,
        sum(monthly_fee) over w_part running_monthly_fee,
        rank() over (order by monthly_fee desc) monthly_fee_rank,
        lag(monthly_fee, 1) over (partition by status, plan order by start_date, id) prev_monthly_fee,
        percent_rank() over (partition by status order by monthly_fee desc nulls first) monthly_fee_percent_rank_2
    from cleaned
    window w_part as (partition by status, plan order by end_date, id rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by plan order by monthly_fee_rank desc, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by plan order by prev_monthly_fee desc nulls last, id) as rn
        from latest
    ) t
    where rn = 1
)
select
    id,
    plan,
    end_date,
    monthly_fee_percent_rank,
    monthly_fee_rank,
    prev_monthly_fee,
    dedup_rank,
    rn,
    case when plan rlike '^(premium|enterprise)$' then 'OTHER_ZONE' when plan rlike '^(basic|standard)$' then 'EXTENDED' else 'CORE' end as plan_zone
from deduped
