with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), starts_on) as days_since_starts_on,
        initcap(trim(promo_code)) as promo_code_title,
        coalesce(channel, 'DIRECT') as channel_filled
    from promotions
    where year(ends_on) >= 2025
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        promo_code,
        ends_on,
        days_since_starts_on,
        channel_filled,
        sum(discount_pct) over w_ordered as running_sum_discount_pct,
        avg(discount_pct) over w_ordered as running_avg_discount_pct,
        min(discount_pct) over w_ordered as running_min_discount_pct,
        row_number() over (partition by promo_code order by days_since_starts_on, id) as seq_no
    from filtered
    window w_ordered as (partition by promo_code order by ends_on, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select *, trim(promo_code) as clean_promo_code
    from enriched
    where running_min_discount_pct < 0.14
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promo_code order by days_since_starts_on desc nulls last, id) as latest_rank
        from base
    ) t
    where latest_rank = 1
)
-- Ranking and gap features
select
    id,
    promo_code,
    ends_on,
    channel_filled,
    running_sum_discount_pct,
    running_avg_discount_pct,
    running_min_discount_pct,
    seq_no,
    latest_rank,
    lag(running_min_discount_pct) over w_recent as prev_running_min_discount_pct,
    lead(running_min_discount_pct) over w_recent as next_running_min_discount_pct,
    lead(days_since_starts_on) over w_recent as next_days_since_starts_on,
    row_number() over w_recent as event_seq,
    round(running_min_discount_pct / sum(running_min_discount_pct) over (partition by date_trunc('YEAR', ends_on), promo_code), 4) as running_min_discount_pct_share
from latest
window w_recent as (partition by promo_code order by latest_rank, id);
