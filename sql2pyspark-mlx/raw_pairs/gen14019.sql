with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        paid_at,
        max(amount) over (partition by status order by cast(paid_at as bigint) asc range between 2592000 preceding and current row) as amount_max_30d,
        row_number() over (order by amount asc, id asc) as row_seq
    from payments
    where order_id in (1, 7, 39)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        paid_at,
        row_seq,
        lead(row_seq) over (partition by order_id order by amount_max_30d asc, id asc) as next_row_seq,
        lag(amount_max_30d) over (partition by order_id order by amount_max_30d asc, id asc) as prev_amount_max_30d,
        lead(amount_max_30d) over (partition by order_id order by amount_max_30d asc, id asc) as next_amount_max_30d,
        row_number() over (partition by order_id order by amount_max_30d asc, id asc) as row_seq_2,
        row_number() over (partition by order_id order by paid_at desc, id asc) as row_seq_3
    from enriched
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        row_seq,
        row_seq_2,
        row_seq_3,
        row_seq_2 + next_amount_max_30d as row_seq_2_plus_next_amount_max_30d,
        prev_amount_max_30d - next_amount_max_30d as prev_amount_max_30d_minus_next_amount_max_30d
    from with_windows
    where paid_at >= '2023-05-01'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(prev_amount_max_30d_minus_next_amount_max_30d, 0) as prev_amount_max_30d_minus_next_amount_max_30d_filled,
        row_seq_2_plus_next_amount_max_30d - prev_amount_max_30d_minus_next_amount_max_30d as row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d
    from cleaned
    where prev_amount_max_30d_minus_next_amount_max_30d > 72
),
with_windows_payments as (
    -- Window metrics over several different partitions
    select
        id,
        prev_amount_max_30d_minus_next_amount_max_30d,
        prev_amount_max_30d_minus_next_amount_max_30d_filled,
        row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d,
        row_number() over (order by prev_amount_max_30d_minus_next_amount_max_30d asc, id asc) as row_seq_4,
        rank() over w_ordered as row_seq_rank,
        cume_dist() over (order by prev_amount_max_30d_minus_next_amount_max_30d desc) as prev_amount_max_30d_minus_next_amount_max_30d_cume_dist,
        dense_rank() over w_grp as row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d_dense_rank
    from valid
    window w_ordered as (order by row_seq asc), w_grp as (order by row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d desc)
)
select
    id,
    row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d,
    row_seq_4,
    prev_amount_max_30d_minus_next_amount_max_30d_cume_dist,
    row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d_dense_rank
from with_windows_payments
order by prev_amount_max_30d_minus_next_amount_max_30d_cume_dist asc nulls last, row_seq_4 asc
