with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        round(discount_pct * 1.1, 2) as adjusted_discount_pct,
        cast(discount_pct as int) as discount_pct_int,
        coalesce(channel, 'DIRECT') as channel_clean
    from promotions
    where ends_on >= '2025-05-01'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        starts_on,
        discount_pct,
        adjusted_discount_pct
    from (
        select *, row_number() over (partition by channel, promo_code order by discount_pct desc, id asc) as row_num
        from cleaned
    ) t
    where row_num = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, date_sub(starts_on, 71) as starts_on_minus_71d
    from deduped
    where adjusted_discount_pct <= 40
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        discount_pct,
        adjusted_discount_pct,
        starts_on_minus_71d,
        case when adjusted_discount_pct >= 80 then 'High' when adjusted_discount_pct >= 28 then 'Medium' else 'Low' end as adjusted_discount_pct_level,
        datediff(starts_on, starts_on_minus_71d) / 7 as weeks_starts_on_minus_71d_to_starts_on
    from valid
    where starts_on >= date '2023-07-20'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(adjusted_discount_pct, 1) over (order by adjusted_discount_pct asc, id asc) as next_adjusted_discount_pct,
        cume_dist() over (order by adjusted_discount_pct desc) as adjusted_discount_pct_cume_dist,
        percent_rank() over (order by adjusted_discount_pct desc nulls first) as adjusted_discount_pct_percent_rank
    from filtered
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        next_adjusted_discount_pct,
        discount_pct,
        adjusted_discount_pct_cume_dist
    from metrics
) src
unpivot (
    reading for metric in (next_adjusted_discount_pct as NEXT_ADJUSTED_DISCOUNT_PCT_VAL, discount_pct as DISCOUNT_PCT, adjusted_discount_pct_cume_dist as ADJUSTED_DISCOUNT_PCT_CUME_DIST)
);
