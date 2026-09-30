with with_gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '60' day then 1 else 0 end as new_group_flag
    from payments
),
sessionized as (
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
flagged as (
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval '7' day then 1 else 0 end as is_new_burst
    from sessionized
),
sessionized_payments as (
    select
        id,
        order_id,
        method,
        paid_at,
        sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by method order by paid_at nulls last, id) as dedup_rank
        from sessionized_payments
    ) x
    where dedup_rank = 1
),
with_gaps_2 as (
    select *, case when lag(paid_at) over (partition by order_id order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at, id) + interval '7' day then 1 else 0 end as is_new_burst
    from deduped
),
sessionized_payments_2 as (
    select *, sum(is_new_burst) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps_2
),
enriched as (
    select
        *,
        avg(is_new_burst) over (order by session_no, id rows between 6 preceding and 1 preceding) as is_new_burst_avg_6_prior,
        sum(session_no) over (partition by order_id, method order by paid_at, id rows between unbounded preceding and current row) as cumulative_session_no,
        last_value(method) ignore nulls over (partition by order_id, method order by session_no, id rows between unbounded preceding and current row) as last_known_method
    from sessionized_payments_2
)
select
    id,
    method,
    paid_at,
    session_id,
    dedup_rank,
    is_new_burst_avg_6_prior,
    cumulative_session_no,
    last_known_method,
    case when method rlike '^(bank_transfer|promptpay)$' then 'GROUP_B' when method rlike '^(card|paypal)$' then 'PRIORITY' else 'EXTENDED' end as method_zone
from enriched
order by is_new_burst_avg_6_prior desc nulls first;
