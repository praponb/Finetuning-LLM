with enriched as (
    -- Ranking and gap features
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        avg(discount_pct) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as running_avg_discount_pct,
        count(discount_pct) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as running_count_discount_pct,
        min(discount_pct) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as running_min_discount_pct
    from promotions
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        starts_on,
        discount_pct,
        running_avg_discount_pct,
        running_count_discount_pct,
        lag(running_count_discount_pct) over w_grp as prev_running_count_discount_pct,
        lead(running_min_discount_pct) over w_grp as next_running_min_discount_pct,
        lag(running_min_discount_pct) over w_grp as prev_running_min_discount_pct,
        row_number() over w_grp as event_seq
    from enriched
    window w_grp as (partition by channel order by running_min_discount_pct, id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from with_windows
    qualify row_number() over (partition by channel order by running_avg_discount_pct desc, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(prev_running_count_discount_pct / sum(prev_running_count_discount_pct) over (partition by month(starts_on)), 4) as prev_running_count_discount_pct_share,
        rank() over (partition by channel order by running_avg_discount_pct desc nulls last) as running_avg_discount_pct_rank
    from ranked
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        channel,
        prev_running_count_discount_pct_share,
        discount_pct,
        next_running_min_discount_pct,
        prev_running_min_discount_pct
    from metrics
) src
unpivot (
    value for measure in (prev_running_count_discount_pct_share as PREV_RUNNING_COUNT_DISCOUNT_PCT_SHARE_VAL, discount_pct as DISCOUNT_PCT_VAL, next_running_min_discount_pct as NEXT_RUNNING_MIN_DISCOUNT_PCT, prev_running_min_discount_pct as PREV_RUNNING_MIN_DISCOUNT_PCT_VAL)
)
