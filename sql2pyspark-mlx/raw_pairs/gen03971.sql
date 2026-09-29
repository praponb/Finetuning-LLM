with base as (
    select
        *,
        round(amount, 2) as amount_rounded,
        coalesce(method, 'OTHER') as method_filled
    from payments
    where paid_at > timestamp '2023-06-24 14:00:00'
      and method is distinct from 'cash'
),
deduped as (
    select
        id,
        paid_at,
        status
    from (
        select *, row_number() over (partition by method order by paid_at nulls first, id) as latest_rank
        from base
    ) t
    where latest_rank = 1
),
valid as (
    select
        *,
        cast(paid_at as date) as paid_at_day,
        case when status in ('FAILED', 'REFUNDED') then 'Core' else 'Other' end as status_group
    from deduped
    where paid_at >= to_timestamp('2023-08-09 05:00:00')
),
latest as (
    select
        id,
        status,
        status_group
    from (
        select *, rank() over (partition by status order by paid_at_day) as rn
        from valid
    ) ranked
    where rn = 1
),
filtered as (
    select *, upper(status) as status_upper
    from latest
    where status = 'FAILED'
),
grouped as (
    select
        status,
        count(case when status = 'REFUNDED' then 1 end) as refunded_count,
        count(distinct id) as distinct_ids,
        sort_array(collect_set(status)) as status_list,
        count(case when status = 'REFUNDED' then 1 end) as refunded_count_2,
        count(distinct status) as distinct_status
    from filtered
    group by status
    having distinct_ids > 1
)
select
    status,
    refunded_count,
    distinct_ids,
    refunded_count_2
from grouped
