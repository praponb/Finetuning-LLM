with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when status in ('SUCCESS', 'REFUNDED') then 'Core' else 'Other' end status_group,
        lower(method) method_lower,
        coalesce(method, 'N/A') resolved_method
    from payments
    where paid_at > to_date('2023-10-14')
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        floor(amount) amount_whole,
        coalesce(method, 'N/A') method_filled
    from base
    where method is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        amount,
        status_group,
        method_lower,
        amount_whole,
        method_filled,
        date_format(paid_at, 'yyyy-MM') paid_at_label,
        day(paid_at) paid_at_day,
        coalesce(method, 'UNKNOWN') resolved_method_2
    from cleaned
    where method is distinct from 'bank_transfer'
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        method,
        method_lower,
        method_filled
    from (
        select *, row_number() over (partition by method order by amount desc nulls first, id) as rn
        from filtered
    ) t
    where rn = 1
)
-- Multi-level aggregation
select
    method,
    order_id,
    count_if(method = 'cash') as cash_events,
    count(*) as cnt,
    count(case when method = 'card' then 1 end) as card_count,
    count(distinct order_id) as distinct_order_ids,
    grouping(method) as gid
from latest
group by grouping sets ((method, order_id), (method))
having distinct_order_ids < 1000
order by method desc nulls first;
