with valid as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        round(datediff(starts_on, ends_on) / 30, 1) as months_ends_on_to_starts_on,
        coalesce(channel, 'DIRECT') as channel_clean
    from promotions
    where channel is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over w_hist as months_ends_on_to_starts_on_dense_rank,
        row_number() over w_grp as event_seq,
        row_number() over w_ordered as row_seq
    from valid
    where promo_code = 'VIP'
    window w_hist as (partition by month(starts_on), promo_code order by months_ends_on_to_starts_on desc), w_grp as (order by months_ends_on_to_starts_on desc, id), w_ordered as (partition by channel order by months_ends_on_to_starts_on, id)
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        discount_pct,
        months_ends_on_to_starts_on_dense_rank,
        row_number() over (partition by month(starts_on), channel order by row_seq, id) as event_seq_2,
        ntile(10) over (partition by promo_code order by months_ends_on_to_starts_on_dense_rank desc, id) as months_ends_on_to_starts_on_dense_rank_bucket
    from metrics
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by promo_code, channel order by months_ends_on_to_starts_on_dense_rank_bucket desc, id) as row_num
    from with_windows
    qualify row_num = 1
)
-- Ranking and gap features
select
    id,
    channel,
    promo_code,
    discount_pct,
    months_ends_on_to_starts_on_dense_rank,
    event_seq_2,
    row_num,
    sum(event_seq_2) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as running_event_seq_2,
    avg(discount_pct) over (partition by promo_code, channel order by row_num, id rows between 3 preceding and 1 preceding) as discount_pct_avg_3_prior,
    last_value(event_seq_2) ignore nulls over (order by row_num, id rows between unbounded preceding and current row) as last_known_event_seq_2,
    first_value(months_ends_on_to_starts_on_dense_rank, true) over (partition by channel, promo_code order by row_num, id rows between unbounded preceding and current row) as first_known_months_ends_on_to_starts_on_dense_rank
from deduped
