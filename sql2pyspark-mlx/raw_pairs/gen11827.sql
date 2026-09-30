with base as (
    -- Basic cleansing and defaults
    select
        *,
        quarter(ends_on) as ends_on_quarter,
        datediff(starts_on, ends_on) / 7 as weeks_ends_on_to_starts_on
    from promotions
    where channel = 'store'
      and channel is not null
),
base_promotions as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        ends_on_quarter,
        weeks_ends_on_to_starts_on,
        round(ends_on_quarter, 2) as ends_on_quarter_rounded,
        discount_pct / nullif(weeks_ends_on_to_starts_on, 0) as discount_pct_per_weeks_ends_on_to_starts_on
    from base
    where ends_on > date '2023-09-09'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        count(discount_pct) over (partition by channel) as channel_count_discount_pct,
        first_value(weeks_ends_on_to_starts_on, true) over (partition by channel order by ends_on_quarter_rounded, id rows between unbounded preceding and current row) as first_known_weeks_ends_on_to_starts_on
    from base_promotions
),
scored as (
    -- Ranking and gap features
    select
        id,
        channel,
        starts_on,
        discount_pct,
        discount_pct_per_weeks_ends_on_to_starts_on,
        first_known_weeks_ends_on_to_starts_on,
        last_value(ends_on_quarter, true) over w_running as last_known_ends_on_quarter,
        sum(ends_on_quarter) over (partition by channel order by ends_on_quarter_rounded, id rows between unbounded preceding and current row) as cumulative_ends_on_quarter
    from metrics
    window w_running as (partition by channel order by channel_count_discount_pct, id rows between unbounded preceding and current row)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        avg(discount_pct_per_weeks_ends_on_to_starts_on) over (partition by year(starts_on) order by last_known_ends_on_quarter, id rows between 6 preceding and 1 preceding) as discount_pct_per_weeks_ends_on_to_starts_on_avg_6_prior,
        last_value(channel) ignore nulls over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as last_known_channel
    from scored
)
-- Aggregate with conditional metrics
select
    channel,
    sort_array(collect_set(channel)) as channel_list,
    sort_array(collect_set(channel)) as channel_list_2
from enriched
group by channel
order by channel desc;
