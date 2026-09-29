with ranked as (
    select *
    from (
        select *, row_number() over (partition by order_id order by paid_at desc nulls first, id) as row_num
        from payments
    ) ranked
    where row_num = 1
),
with_gaps as (
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval '30' day then 1 else 0 end as new_group_flag
    from ranked
),
sessionized as (
    select *, sum(new_group_flag) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
cleaned as (
    select *, least(row_num, session_no) as least_row_num_session_no
    from sessionized
    where order_id is not null
),
scored as (
    select
        *,
        lead(least_row_num_session_no) over (partition by method order by paid_at, id) as next_least_row_num_session_no,
        lead(new_group_flag) over (partition by method order by paid_at, id) as next_new_group_flag,
        row_number() over (partition by method order by paid_at, id) as seq_no,
        avg(new_group_flag) over (partition by to_date(paid_at) order by paid_at, id rows between 3 preceding and 1 preceding) as new_group_flag_avg_3_prior
    from cleaned
    where status in ('REFUNDED', 'FAILED', 'SUCCESS')
),
enriched as (
    select
        src.*,
        o.shipping_fee,
        o.discount,
        o.status as order_status
    from scored src
    inner join orders o
        on src.order_id = o.id
)
select
    id,
    amount,
    paid_at,
    status,
    new_group_flag,
    session_no,
    least_row_num_session_no,
    next_least_row_num_session_no,
    next_new_group_flag
from enriched
order by id nulls last;
