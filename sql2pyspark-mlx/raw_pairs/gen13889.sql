with valid as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        case when channel in ('mobile', 'partner') then 'Tier 1' else 'Other' end as channel_group,
        trim(promo_code) as clean_promo_code
    from promotions
    where starts_on > to_date('2023-07-26')
      and channel is not null
),
filtered as (
    select *, date_format(starts_on, 'yyyy-MM') as starts_on_label
    from valid
    where starts_on >= date '2023-07-13'
      and starts_on >= '2024-10-01'
),
latest as (
    select *
    from (
        select *, row_number() over (partition by promo_code order by starts_on desc, id) as dedup_rank
        from filtered
    ) t
    where dedup_rank = 1
),
base as (
    select *, date_trunc('year', starts_on) as starts_on_period
    from latest
    where starts_on > date '2023-08-27'
),
report as (
    select
        id,
        starts_on,
        ends_on,
        channel_group,
        clean_promo_code,
        starts_on_label,
        starts_on_period
    from base
    order by starts_on_period nulls first, ends_on desc
)
select
    id,
    ends_on,
    channel_group,
    clean_promo_code,
    starts_on_period
from report
