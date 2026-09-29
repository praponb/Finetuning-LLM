WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(channel, 'Unknown') AS channel_filled,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-05-31')
      AND channel IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN discount_pct ELSE 0 END) AS bundle_discount_pct,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS discount_pct_per_bundle,
    COUNT(*) AS row_count,
    GROUPING(DATE_TRUNC('YEAR', ends_on)) AS grouping_id
FROM filtered
GROUP BY CUBE (DATE_TRUNC('YEAR', ends_on))
ORDER BY bundle_discount_pct
