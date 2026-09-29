WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(channel, ' ', promo_code) AS channel_promo_code
    FROM promotions
    WHERE channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        channel_promo_code,
        MAX(discount_pct) OVER w_running AS channel_max_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        STDDEV_SAMP(discount_pct) OVER w_grp AS promo_code_stddev_samp_discount_pct
    FROM valid
    WINDOW w_running AS (PARTITION BY channel), w_grp AS (PARTITION BY promo_code)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        promo_code_stddev_samp_discount_pct + channel_max_discount_pct AS promo_code_stddev_samp_discount_pct_plus_channel_max_discount_pct,
        ROUND(discount_pct, 1) AS discount_pct_rounded
    FROM with_windows
    WHERE promo_code != 'SPRING10'
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        promo_code_stddev_samp_discount_pct * discount_pct_rounded AS promo_code_stddev_samp_discount_pct_times_discount_pct_rounded,
        TRIM(promo_code) AS clean_promo_code
    FROM base
    WHERE ends_on > '2023-08-23'
),
valid_promotions AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label
    FROM base_promotions
    WHERE ends_on > DATE '2023-08-21'
      AND promo_code_stddev_samp_discount_pct_times_discount_pct_rounded <= 53
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promo_code_stddev_samp_discount_pct >= 0 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_code_stddev_samp_discount_pct_tier,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_per_welcome5,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    AVG(promo_code_stddev_samp_discount_pct_times_discount_pct_rounded) AS avg_promo_code_stddev_samp_discount_pct_times_discount_pct_rounded,
    SUM(discount_pct) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_total_discount_pct,
    AVG(discount_pct_share) AS avg_discount_pct_share
FROM valid_promotions
GROUP BY CASE WHEN promo_code_stddev_samp_discount_pct >= 0 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SUM(discount_pct) FILTER (WHERE promo_code = 'BUNDLE') < 1000000;
