with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at, id) > interval '14' day then 1 else 0 end as is_new_session
    from payments
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        first_value(amount, true) over (partition by method order by burst_id, id rows between unbounded preceding and current row) as first_known_amount,
        row_number() over (order by burst_id, id) as seq_no
    from islands
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval '30' day then 1 else 0 end as is_new_session_2
    from metrics
),
islands_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session_2) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(burst_id, 1) over (partition by method order by seq_no, id) as next_burst_id,
        lead(is_new_session, 2) over (partition by method, status order by is_new_session, id) as next_is_new_session,
        count(first_known_amount) over (partition by method) as method_count_first_known_amount,
        sum(seq_no) over (partition by status order by is_new_session_2, id rows between unbounded preceding and current row) as running_seq_no,
        avg(session_no) over (partition by order_id order by is_new_session_2, id rows between 3 preceding and 1 preceding) as session_no_avg_3_prior
    from islands_payments
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        o.shipping_fee,
        o.channel
    from with_windows src
    join orders o
        on src.order_id = o.id
),
enriched as (
    -- Attach orders attributes
    select e.*, p.status as order_status
    from with_dims e
    left join orders p
        on e.order_id = p.id
)
select
    s.id,
    s.order_id,
    s.amount,
    s.status,
    s.is_new_session,
    s.burst_id,
    s.seq_no,
    s.next_is_new_session,
    s.running_seq_no,
    case when status rlike '^(REFUNDED|SUCCESS)$' then 'EXTENDED' when status rlike '^(FAILED)$' then 'GROUP_A' else 'PRIORITY' end as status_zone,
    (select avg(x.amount) from payments x where x.order_id = s.order_id) as avg_payment_amount
from enriched s
order by burst_id nulls last;
