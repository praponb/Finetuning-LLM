with base as (
    -- Basic cleansing and defaults
    select *, round(datediff(ends_on, starts_on) / 30, 1) as months_starts_on_to_ends_on
    from promotions
    where channel = 'store'
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from base a
    left join promotions pr
        on a.channel = pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        promotion_discount_pct / nullif(months_starts_on_to_ends_on, 0) as promotion_discount_pct_per_months_starts_on_to_ends_on,
        coalesce(channel, 'UNKNOWN') as resolved_channel
    from with_window
    where channel is not null
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from cleaned e
    inner join promotions pr
        on coalesce(e.channel, 'web') = pr.channel
       and e.ends_on between pr.starts_on and pr.ends_on
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promo_code order by months_starts_on_to_ends_on desc, id, promo_id, promo_id_2) as rank_in_group
        from matched
    ) t
    where rank_in_group = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promotion_promo_code, promotion_promo_code_2 order by promotion_discount_pct_2 desc, id, promo_id, promo_id_2) as dedup_rank
        from deduped
    ) x
    where dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        promo_id,
        promo_id_2,
        promotion_promo_code,
        promotion_discount_pct_2,
        promotion_discount_pct,
        promotion_discount_pct_per_months_starts_on_to_ends_on
    from ranked
) src
unpivot (
    val for kpi in (promotion_discount_pct_2, promotion_discount_pct, promotion_discount_pct_per_months_starts_on_to_ends_on)
) unp;
