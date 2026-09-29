with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        add_months(starts_on, 1) as starts_on_plus_months,
        year(starts_on) as starts_on_year,
        coalesce(channel, 'DIRECT') as channel_clean
    from promotions
    where channel is not null
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        length(channel) as channel_length,
        datediff(ends_on, starts_on) as days_starts_on_to_ends_on
    from filtered
    where channel is not null
      and starts_on_plus_months > '2023-05-01'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        starts_on_plus_months,
        starts_on_year,
        channel_clean,
        days_starts_on_to_ends_on,
        avg(starts_on_year) over w_running as running_avg_starts_on_year,
        count(starts_on_year) over w_running as running_count_starts_on_year,
        max(starts_on_year) over w_running as running_max_starts_on_year
    from base
    where starts_on_year <= 79
    window w_running as (partition by promo_code order by starts_on_year, id rows between unbounded preceding and current row)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by month(starts_on_plus_months), channel order by starts_on_year, id) as seq_no,
        ntile(4) over (partition by date_trunc('MONTH', starts_on_plus_months), channel order by running_count_starts_on_year desc, id) as running_count_starts_on_year_bucket,
        row_number() over (partition by year(starts_on_plus_months), channel order by days_starts_on_to_ends_on, id) as event_seq
    from with_windows
),
matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from scored src
    left join promotions pr
        on src.channel = pr.channel
       and src.starts_on_plus_months >= pr.starts_on
       and src.starts_on_plus_months <= date_add(pr.ends_on, 7)
)
select
    id,
    channel,
    starts_on_plus_months,
    starts_on_year,
    days_starts_on_to_ends_on,
    running_max_starts_on_year,
    running_count_starts_on_year_bucket,
    promo_id,
    discount_pct,
    case when channel rlike '^(partner|web)$' then 'OTHER_ZONE' when channel rlike '^(mobile|store)$' then 'EXTENDED' else 'GROUP_A' end as channel_zone
from matched
order by channel_zone desc nulls first;
