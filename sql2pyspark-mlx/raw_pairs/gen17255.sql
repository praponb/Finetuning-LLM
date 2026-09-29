with cleaned as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        case when discount_pct >= 0.3 then 'High' when discount_pct >= 0.13 then 'Medium' else 'Low' end discount_pct_tier,
        coalesce(channel, 'OTHER') channel_filled
    from promotions
    where channel is not null
      and channel is distinct from 'store'
),
ranked as (
    select *
    from cleaned
    qualify row_number() over (partition by promo_code order by ends_on, id) = 1
),
remaining_keys as (
    select id, channel
    from ranked
    except distinct
    select id, channel
    from promotions
    where discount_pct > 0.21
),
reconciled as (
    select s.*
    from ranked s
    inner join remaining_keys k on s.id = k.id and s.channel = k.channel
),
filtered as (
    select *, upper(channel) channel_upper
    from reconciled
    where starts_on >= date '2023-09-20'
)
select
    date_trunc('month', ends_on) as activity_month,
    promo_code,
    size(collect_set(channel)) as num_channels,
    count_if(promo_code = 'SPRING10') as spring10_events,
    count(*) as event_count,
    size(collect_set(channel)) as num_channels_2,
    count(*) as cnt,
    sort_array(collect_set(promo_code)) as promo_code_list,
    grouping(date_trunc('month', ends_on)) as gid
from filtered
group by rollup (date_trunc('month', ends_on), promo_code)
having spring10_events < 1000;
