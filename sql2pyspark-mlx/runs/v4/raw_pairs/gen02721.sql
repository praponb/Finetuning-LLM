WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAY(ends_on) AS ends_on_day
    FROM promotions
    WHERE ends_on > TO_DATE('2024-05-23')
      AND channel IS NOT NULL
      AND channel IN ('store', 'web')
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        ROUND(discount_pct, 0) AS discount_pct_rounded
    FROM cleaned
    WHERE starts_on >= DATE '2023-07-31'
      AND channel IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        ends_on_day,
        days_ends_on_to_starts_on,
        discount_pct_rounded,
        DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on,
        FLOOR(discount_pct) AS discount_pct_whole
    FROM cleaned_promotions
    WHERE channel IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN ends_on_day >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS ends_on_day_tier,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    SUM(days_ends_on_to_starts_on) FILTER (WHERE channel = 'partner') AS partner_total_days_ends_on_to_starts_on,
    PERCENTILE_APPROX(discount_pct_rounded, 0.9) AS discount_pct_rounded_p90,
    GROUPING_ID() AS gid
FROM valid
GROUP BY ROLLUP (CASE WHEN ends_on_day >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY partner_total_days_ends_on_to_starts_on DESC;
