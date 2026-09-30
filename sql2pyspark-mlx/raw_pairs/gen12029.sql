with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '90' day then 1 else 0 end as session_start_flag
    from payments
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        lag(session_id) over w_grp as prev_session_id,
        lead(session_id) over w_grp as next_session_id,
        lead(session_start_flag) over w_grp as next_session_start_flag,
        lag(session_start_flag) over w_grp as prev_session_start_flag,
        row_number() over w_grp as seq_no
    from islands
    window w_grp as (partition by method order by session_id, id)
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 60 days then 1 else 0 end as is_new_burst
    from scored
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        paid_at,
        status,
        next_session_start_flag,
        seq_no,
        sum(is_new_burst) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged_2
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        next_session_start_flag,
        seq_no,
        island_id,
        lead(seq_no, 1) over (partition by order_id order by island_id, id) as next_seq_no,
        max(island_id) over (partition by status, order_id) as status_max_island_id,
        max(seq_no) over (partition by order_id order by cast(paid_at as bigint) range between 2592000 preceding and current row) as seq_no_max_30d
    from sessionized
    where island_id between 3 and 8
)
select
    s.id,
    s.order_id,
    s.next_session_start_flag,
    s.seq_no,
    s.island_id,
    s.next_seq_no,
    s.status_max_island_id,
    s.seq_no_max_30d,
    (select avg(x.quantity) from order_items x where x.order_id = s.order_id) as avg_order_item_quantity
from with_windows s
order by avg_order_item_quantity desc nulls first
