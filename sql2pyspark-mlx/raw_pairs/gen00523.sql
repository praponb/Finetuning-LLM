with base as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        concat_ws('-', status, method) as status_method_key
    from payments
    where order_id in (2, 3, 9, 32)
),
ranked as (
    select id, order_id
    from base
    qualify rank() over (partition by status order by amount desc nulls last) = 1
),
with_dims as (
    select
        e.id,
        e.order_id,
        p.amount,
        p.status
    from ranked e
    left join orders p
        on e.order_id = p.id
),
with_windows as (
    select
        id,
        status,
        lag(amount) over (partition by order_id, status order by amount, id) as prev_amount,
        lead(amount) over (partition by order_id, status order by amount, id) as next_amount,
        row_number() over (partition by order_id, status order by amount, id) as seq_no,
        row_number() over (partition by status order by amount desc, id) as event_seq
    from with_dims
),
scored as (
    select
        *,
        first_value(prev_amount) ignore nulls over (order by event_seq, id rows between unbounded preceding and current row) as first_known_prev_amount,
        row_number() over (partition by status order by prev_amount desc, id) as event_seq_2,
        first_value(prev_amount, true) over (partition by status order by next_amount, id rows between unbounded preceding and current row) as first_known_prev_amount_2,
        first_value(status) ignore nulls over (partition by status order by event_seq, id rows between unbounded preceding and current row) as first_known_status
    from with_windows
),
deduped as (
    select
        id,
        prev_amount,
        seq_no,
        first_known_prev_amount_2
    from (
        select *, rank() over (partition by status order by event_seq_2 desc nulls last) as row_num
        from scored
    ) t
    where row_num = 1
)
select
    seq_no,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    approx_percentile(first_known_prev_amount_2, array(0.25, 0.5, 0.75)) as first_known_prev_amount_2_quartiles,
    count(*) as cnt
from deduped
group by seq_no;
