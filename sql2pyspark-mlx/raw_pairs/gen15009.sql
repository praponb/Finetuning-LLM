WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(discount_pct * 1.07, 2) AS discount_pct_adjusted
    FROM promotions
    WHERE starts_on > DATE '2023-08-14'
      AND starts_on < '2025-08-29'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct_adjusted) OVER (PARTITION BY promo_code) AS promo_code_avg_discount_pct_adjusted,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        LAG(discount_pct_adjusted, 1) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct_adjusted,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct) AS discount_pct_cume_dist,
        AVG(discount_pct_adjusted) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_adjusted_avg_3_prior
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        discount_pct_adjusted,
        promo_code_avg_discount_pct_adjusted,
        prev_discount_pct_adjusted,
        discount_pct_cume_dist,
        discount_pct_adjusted_avg_3_prior,
        AVG(prev_discount_pct_adjusted) OVER w_ordered AS channel_avg_prev_discount_pct_adjusted,
        SUM(promo_code_avg_discount_pct_adjusted) OVER w_grp AS running_promo_code_avg_discount_pct_adjusted
    FROM scored
    WINDOW w_ordered AS (PARTITION BY channel), w_grp AS (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM promotions
    WHERE discount_pct BETWEEN 0.24 AND 0.27
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, running_promo_code_avg_discount_pct_adjusted / NULLIF(discount_pct_cume_dist, 0) AS running_promo_code_avg_discount_pct_adjusted_per_discount_pct_cume_dist
    FROM verified
    WHERE discount_pct_cume_dist <= 0.4
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        promo_code,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
        ROUND(SUM(discount_pct_adjusted) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS discount_pct_adjusted_per_bundle
    FROM valid
    GROUP BY 1
)
SELECT
    promo_code,
    promo_code_list,
    discount_pct_adjusted_per_bundle,
    CASE WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'EXTENDED' WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS promo_code_zone
FROM stats
ORDER BY promo_code_zone DESC;
