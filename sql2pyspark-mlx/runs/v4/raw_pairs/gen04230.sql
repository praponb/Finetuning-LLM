with attributed as (
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions s
    left join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 7)
),
enriched as (
    select
        *,
        first_value(channel) ignore nulls over (partition by promotion_promo_code order by discount_pct, id, promo_id rows between unbounded preceding and current row) as first_known_channel,
        round(promo_id / sum(promo_id) over (partition by channel), 4) as promo_id_share
    from attributed
),
filtered as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        first_known_channel,
        weekofyear(starts_on) as starts_on_weekofyear,
        coalesce(channel, 'DIRECT') as channel_clean
    from enriched
    where channel is not null
),
base as (
    select
        id,
        channel,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        first_known_channel,
        channel_clean,
        date_sub(starts_on, 73) as starts_on_minus_73d
    from filtered
    where starts_on >= date '2023-08-28'
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by channel order by promo_id desc nulls first, id, promo_id) as dedup_rank
        from base
    ) t
    where dedup_rank = 1
),
metrics as (
    select
        *,
        lead(promo_id, 2) over (partition by channel order by promo_id, id, promo_id) as next_promo_id,
        first_value(promo_id, true) over (partition by promotion_promo_code order by ends_on, id, promo_id rows between unbounded preceding and current row) as first_known_promo_id
    from ranked
),
daily as (
    select
        date_trunc('week', ends_on) as week_start,
        date_trunc('quarter', ends_on) as quarter_start,
        round(avg(discount_pct), 2) as avg_discount_pct,
        sum(discount_pct) as total_discount_pct,
        count(distinct id) as distinct_ids,
        percentile_approx(discount_pct, 0.95) as discount_pct_p95,
        count(distinct promotion_promo_code) as distinct_promotion_promo_codes,
        percentile_approx(next_promo_id, array(0.25, 0.5, 0.75)) as next_promo_id_quartiles
    from metrics
    group by week_start, quarter_start
    having distinct_promotion_promo_codes > 1
)
select
    week_start,
    quarter_start,
    avg_discount_pct,
    total_discount_pct,
    distinct_ids,
    discount_pct_p95,
    distinct_promotion_promo_codes
from daily;
