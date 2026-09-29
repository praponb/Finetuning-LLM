with filtered as (
    select *, length(method) method_length
    from payments
    where paid_at >= timestamp '2023-12-17 23:00:00'
),
cleaned as (
    select
        id,
        method,
        paid_at,
        status,
        method_length,
        method_length - amount method_length_minus_amount,
        coalesce(method, 'OTHER') method_clean
    from filtered
    where paid_at > date '2023-10-10'
),
enriched as (
    select
        id,
        method,
        paid_at,
        status,
        method_length,
        method_length_minus_amount,
        method_clean,
        sum(method_length_minus_amount) over (partition by status order by method_length asc, id asc rows between unbounded preceding and current row) running_method_length_minus_amount,
        avg(method_length_minus_amount) over (partition by method, status) method_avg_method_length_minus_amount
    from cleaned
),
with_gaps as (
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at asc, id asc) > interval 30 days then 1 else 0 end as session_start_flag
    from enriched
),
sessions_payments as (
    select *, sum(session_start_flag) over (partition by status order by paid_at asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    id,
    method,
    paid_at,
    method_length_minus_amount,
    running_method_length_minus_amount,
    session_start_flag,
    session_id,
    case when method rlike '^(paypal|bank_transfer)$' then 'EXTENDED' else 'OTHER_ZONE' end as method_zone
from sessions_payments
order by session_id asc
