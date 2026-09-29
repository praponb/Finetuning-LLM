with valid as (
    select *, lower(promo_code) as promo_code_lower
    from promotions
    where ends_on > date '2023-08-22'
),
matched_keys as (
    select promo_code
    from valid
    except
    select promo_code
    from promotions
    where promo_code in ('SPRING10', 'WELCOME5', 'BUNDLE')
),
reconciled as (
    select s.*
    from valid s
    join matched_keys k on s.promo_code = k.promo_code
),
filtered as (
    select *, datediff(ends_on, starts_on) / 7 as weeks_starts_on_to_ends_on
    from reconciled
    where channel is not null
      and starts_on > date '2023-04-23'
),
enriched as (
    select
        channel,
        discount_pct,
        weeks_starts_on_to_ends_on,
        rank() over (partition by date_trunc('MONTH', ends_on), promo_code order by weeks_starts_on_to_ends_on desc nulls first) as weeks_starts_on_to_ends_on_rank,
        round(discount_pct / sum(discount_pct) over (partition by month(starts_on), channel), 4) as discount_pct_share,
        stddev_samp(weeks_starts_on_to_ends_on) over (partition by month(ends_on), promo_code) as ends_on_month_stddev_samp_weeks_starts_on_to_ends_on
    from filtered
),
scored as (
    select
        *,
        percent_rank() over (partition by channel order by weeks_starts_on_to_ends_on desc nulls first) as weeks_starts_on_to_ends_on_percent_rank,
        max(ends_on_month_stddev_samp_weeks_starts_on_to_ends_on) over w_running as channel_max_ends_on_month_stddev_samp_weeks_starts_on_to_ends_on,
        count(weeks_starts_on_to_ends_on_rank) over w_running as channel_count_weeks_starts_on_to_ends_on_rank
    from enriched
    window w_running as (partition by channel)
)
select
    channel,
    measure,
    metric_value
from (
    select
        channel,
        weeks_starts_on_to_ends_on,
        weeks_starts_on_to_ends_on_percent_rank
    from scored
) src
unpivot (
    metric_value for measure in (weeks_starts_on_to_ends_on, weeks_starts_on_to_ends_on_percent_rank)
)
