WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS rec
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN element = 'VIP' THEN 1 ELSE 0 END is_vip,
        LOWER(element) element_lower
    FROM exploded_carts
    WHERE promo IN ('SPRING10', 'VIP', 'BUNDLE')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(is_vip) OVER w_running promo_stddev_samp_is_vip,
        MIN(is_vip) OVER w_running promo_min_is_vip,
        PERCENT_RANK() OVER (PARTITION BY element, promo ORDER BY is_vip DESC NULLS LAST) is_vip_percent_rank
    FROM cleaned
    WINDOW w_running AS (PARTITION BY promo)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        promo,
        element_lower,
        promo_stddev_samp_is_vip,
        promo_min_is_vip,
        is_vip_percent_rank,
        COALESCE(element, 'N/A') element_filled,
        COALESCE(promo, 'NONE') resolved_promo
    FROM enriched
    WHERE is_vip <= 42
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        promo,
        element_lower,
        promo_stddev_samp_is_vip,
        promo_min_is_vip,
        is_vip_percent_rank,
        element_filled,
        resolved_promo,
        YEAR(created_at) created_at_year
    FROM base
    WHERE HOUR(created_at) BETWEEN 7 AND 14
      AND promo IS DISTINCT FROM 'VIP'
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(is_vip_percent_rank) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) is_vip_percent_rank_sum_30m,
        MAX(promo_min_is_vip) OVER w_ordered promo_min_is_vip_max_1h,
        MAX(promo_min_is_vip) OVER (PARTITION BY promo) promo_max_promo_min_is_vip,
        MAX(promo_min_is_vip) OVER w_ordered promo_min_is_vip_max_1h_2,
        COUNT(created_at_year) OVER w_hist created_at_year_count_15m
    FROM cleaned_carts
    WINDOW w_ordered AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
SELECT
    created_at,
    promo_min_is_vip,
    element_filled,
    resolved_promo,
    created_at_year,
    promo_min_is_vip_max_1h,
    promo_max_promo_min_is_vip,
    promo_min_is_vip_max_1h_2,
    created_at_year_count_15m
FROM enriched_carts
ORDER BY created_at DESC NULLS LAST;
