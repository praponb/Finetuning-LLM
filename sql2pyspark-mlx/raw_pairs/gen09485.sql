WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ROUND(discount_pct, 0) AS discount_pct_rounded,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-07-01')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS row_num
        FROM valid
    ) t
    WHERE row_num = 1
),
ranked_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_rounded DESC NULLS LAST, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        starts_on,
        discount_pct_rounded,
        resolved_channel,
        row_num
    FROM ranked_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY rank_in_group, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN discount_pct_rounded >= 70 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_rounded_tier,
    DATE_TRUNC('YEAR', starts_on) AS year_start,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(discount_pct_rounded, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_rounded_quartiles,
    ROUND(AVG(discount_pct_rounded), 2) AS avg_discount_pct_rounded,
    GROUPING_ID() AS gid
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN discount_pct_rounded >= 70 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', starts_on)), (CASE WHEN discount_pct_rounded >= 70 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING cnt > 1
