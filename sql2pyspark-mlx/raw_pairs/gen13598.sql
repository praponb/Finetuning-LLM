WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        LENGTH(promo_code) AS promo_code_length
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on >= DATE '2023-07-10'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM cleaned
    WHERE promo_code_length IS NULL
),
cleaned_promotions AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN promo_code_length >= 70 THEN 'High' WHEN promo_code_length >= 36 THEN 'Medium' ELSE 'Low' END AS promo_code_length_level
    FROM valid
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'partner'
),
cleaned_promotions_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code_length,
        channel_title,
        resolved_channel,
        promo_code_length_level,
        CEIL(promo_code_length) AS promo_code_length_whole,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM cleaned_promotions
    WHERE channel IS NOT NULL
      AND channel = 'store'
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN promo_code_length_whole >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_code_length_whole_tier,
        ROUND(AVG(promo_code_length_whole), 2) AS avg_promo_code_length_whole,
        SUM(promo_code_length) FILTER (WHERE channel = 'web') AS web_total_promo_code_length,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(promo_code_length_whole) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS promo_code_length_whole_per_partner,
        APPROX_PERCENTILE(promo_code_length, 0.5) AS promo_code_length_p50
    FROM cleaned_promotions_2
    GROUP BY promo_code_length_whole_tier
)
SELECT
    promo_code_length_whole_tier,
    web_total_promo_code_length,
    distinct_ids,
    promo_code_length_whole_per_partner,
    promo_code_length_p50
FROM totals
ORDER BY web_total_promo_code_length DESC;
