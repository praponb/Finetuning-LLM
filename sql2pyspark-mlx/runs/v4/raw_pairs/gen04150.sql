WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct * 0.9, 2) AS discount_pct_scaled,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE starts_on > DATE '2023-06-14'
      AND channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_scaled,
        resolved_channel,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_scaled DESC, id ASC) AS event_seq,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior
    FROM base
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY event_seq DESC NULLS LAST, id ASC) = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN discount_pct_avg_5_prior >= 0 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_avg_5_prior_tier,
        COUNT(*) AS cnt,
        SUM(CASE WHEN channel = 'web' THEN discount_pct ELSE 0 END) AS web_discount_pct,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS row_seq_per_spring10,
        ROUND(SUM(discount_pct_scaled) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS discount_pct_scaled_per_partner,
        COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
        COUNT(DISTINCT promo_code) AS distinct_promo_codes
    FROM ranked
    GROUP BY CASE WHEN discount_pct_avg_5_prior >= 0 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    discount_pct_avg_5_prior_tier,
    cnt,
    web_discount_pct,
    web_rows
FROM daily
ORDER BY discount_pct_avg_5_prior_tier ASC
LIMIT 5;
