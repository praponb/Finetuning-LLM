with lines as (
    select
        id,
        created_at,
        k,
        value
    from carts
    lateral view explode(attributes) t as k, value
),
metrics as (
    select
        id,
        k,
        value,
        row_number() over (partition by k order by created_at desc, id asc, k asc) as row_seq,
        row_number() over w_grp as seq_no,
        first_value(k) ignore nulls over (partition by month(created_at), k order by created_at asc, id asc, k asc rows between unbounded preceding and current row) as first_known_k,
        row_number() over (partition by k order by created_at asc, id asc, k asc) as seq_no_2,
        last_value(k, true) over w_running as last_known_k
    from lines
    window w_grp as (order by created_at desc, id asc, k asc), w_running as (order by created_at asc, id asc, k asc rows between unbounded preceding and current row)
),
ranked as (
    select *, rank() over (partition by k order by seq_no desc nulls first) as dedup_rank
    from metrics
    qualify dedup_rank = 1
),
base as (
    select *, ceil(dedup_rank) as dedup_rank_whole
    from ranked
    where k is not null
),
base_carts as (
    select
        id,
        k,
        value,
        row_seq,
        seq_no,
        last_known_k,
        dedup_rank,
        round(dedup_rank / seq_no_2, 2) as dedup_rank_per_seq_no_2,
        seq_no_2 - dedup_rank as seq_no_2_minus_dedup_rank
    from base
    where seq_no >= 7
)
select
    id,
    k,
    row_seq,
    dedup_rank_per_seq_no_2,
    seq_no_2_minus_dedup_rank,
    cume_dist() over (partition by k order by seq_no_2_minus_dedup_rank asc) as seq_no_2_minus_dedup_rank_cume_dist,
    lead(dedup_rank_per_seq_no_2, 1) over (partition by k order by dedup_rank_per_seq_no_2 asc, id asc, k asc) as next_dedup_rank_per_seq_no_2,
    row_number() over (order by dedup_rank desc, id asc, k asc) as event_seq
from base_carts
