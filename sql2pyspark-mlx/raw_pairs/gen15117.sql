with scored as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        ends_on,
        rank() over (partition by promo_code, channel order by discount_pct) as discount_pct_rank,
        lead(discount_pct, 1, 0) over w_running as next_discount_pct
    from promotions
    window w_running as (partition by channel order by starts_on, id)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        discount_pct_rank,
        add_months(ends_on, 6) as ends_on_plus_months
    from scored
    where ends_on >= to_date('2023-11-23')
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount_pct_rank) over w_recent as running_sum_discount_pct_rank,
        count(discount_pct_rank) over w_recent as running_count_discount_pct_rank,
        max(discount_pct_rank) over w_recent as running_max_discount_pct_rank,
        first_value(channel, true) over w_part as first_known_channel
    from filtered
    window w_recent as (partition by channel order by discount_pct_rank, id rows between unbounded preceding and current row), w_part as (partition by promo_code, channel order by ends_on_plus_months, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(promo_code)) as promo_code_title,
        round(running_max_discount_pct_rank / running_count_discount_pct_rank, 2) as running_max_discount_pct_rank_per_running_count_discount_pct_rank,
        coalesce(channel, 'N/A') as resolved_channel
    from enriched
    where running_sum_discount_pct_rank is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        discount_pct_rank,
        running_sum_discount_pct_rank,
        running_count_discount_pct_rank,
        running_max_discount_pct_rank,
        promo_code_title,
        resolved_channel,
        ntile(3) over (order by running_count_discount_pct_rank, id) as running_count_discount_pct_rank_bucket,
        avg(running_max_discount_pct_rank_per_running_count_discount_pct_rank) over (partition by channel, promo_code order by running_sum_discount_pct_rank, id rows between 6 preceding and 1 preceding) as running_max_discount_pct_rank_per_running_count_discount_pct_rank_avg_6_prior,
        cume_dist() over (partition by year(ends_on_plus_months), channel order by running_sum_discount_pct_rank desc) as running_sum_discount_pct_rank_cume_dist
    from base
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by channel order by running_sum_discount_pct_rank_cume_dist desc nulls first, id) as rn
        from with_windows
    ) x
    where rn = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by channel order by running_count_discount_pct_rank_bucket desc nulls first, id) as latest_rank
    from deduped
    qualify latest_rank = 1
),
daily as (
    -- Aggregate with conditional metrics
    select
        promo_code,
        count(*) filter (where promo_code = 'VIP') as vip_rows,
        min(running_max_discount_pct_rank) as min_running_max_discount_pct_rank
    from latest
    group by promo_code
    having vip_rows > 1
)
select
    promo_code,
    vip_rows,
    min_running_max_discount_pct_rank,
    case when promo_code rlike '^(WELCOME5|BUNDLE)$' then 'EXTENDED' when promo_code rlike '^(SPRING10|VIP)$' then 'GROUP_A' else 'GROUP_B' end as promo_code_zone
from daily
order by vip_rows nulls first
