WITH latest AS (
    -- QUALIFY-style window filter
    SELECT id, promo_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS latest_rank
        FROM promotions
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(promo_code) AS promo_code_lower,
        LENGTH(promo_code) AS promo_code_length
    FROM latest
    WHERE promo_code IN ('VIP', 'SPRING10', 'BUNDLE')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY promo_code_length DESC) AS promo_code_length_cume_dist,
        NTILE(10) OVER (ORDER BY promo_code_length DESC, id) AS promo_code_length_bucket,
        DENSE_RANK() OVER (ORDER BY promo_code_length NULLS LAST) AS promo_code_length_dense_rank
    FROM base
    WHERE promo_code_length BETWEEN 37 AND 52
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY promo_code_length) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_code_length_dense_rank NULLS LAST, id) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
)
-- Grouped metrics
SELECT
    promo_code,
    CASE WHEN promo_code_length_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS promo_code_length_dense_rank_tier,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    AVG(promo_code_length_dense_rank) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_avg_promo_code_length_dense_rank,
    ROUND(AVG(promo_code_length_bucket), 2) AS avg_promo_code_length_bucket,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY COUNT(*) FILTER (WHERE promo_code = 'SPRING10') DESC) AS spring10_rows_percent_rank
FROM deduped_promotions
GROUP BY promo_code, CASE WHEN promo_code_length_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY spring10_rows
