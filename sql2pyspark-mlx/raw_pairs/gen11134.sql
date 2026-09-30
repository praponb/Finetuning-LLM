WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('week', ends_on), channel ORDER BY discount_pct DESC) AS discount_pct_rank,
        LEAD(discount_pct, 1, 0) OVER (ORDER BY discount_pct, id) AS next_discount_pct,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM promotions
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY next_discount_pct DESC, id) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(promo_code) AS promo_code_lower,
        QUARTER(starts_on) AS starts_on_quarter
    FROM latest
    WHERE starts_on > TO_DATE('2023-06-12')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_lower,
        starts_on_quarter,
        DATE_FORMAT(starts_on, 'MMM yyyy') AS starts_on_label
    FROM cleaned
    WHERE YEAR(starts_on) = 2025
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
)
-- Multi-level aggregation
SELECT
    promo_code,
    DATE_TRUNC('YEAR', starts_on) AS year_start,
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    AVG(discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_discount_pct,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    SUM(CASE WHEN promo_code = 'WELCOME5' THEN discount_pct ELSE 0 END) AS welcome5_discount_pct,
    COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count
FROM deduped
GROUP BY GROUPING SETS ((promo_code, DATE_TRUNC('YEAR', starts_on), DATE_TRUNC('QUARTER', starts_on)), (promo_code, DATE_TRUNC('YEAR', starts_on)), ())
