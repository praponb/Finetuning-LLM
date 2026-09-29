WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        ROUND(discount_pct, 1) AS discount_pct_rounded,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-05-23')
      AND channel IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(is_bundle) IGNORE NULLS OVER w_ordered AS last_known_is_bundle,
        LAST_VALUE(is_bundle) IGNORE NULLS OVER w_recent AS last_known_is_bundle_2,
        PERCENT_RANK() OVER (PARTITION BY MONTH(ends_on), channel ORDER BY discount_pct DESC) AS discount_pct_percent_rank,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM base
    WINDOW w_ordered AS (PARTITION BY channel, promo_code ORDER BY is_bundle, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_rounded DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        is_bundle,
        resolved_channel,
        last_known_is_bundle_2,
        row_num,
        ROUND(last_known_is_bundle_2, 1) AS last_known_is_bundle_2_rounded
    FROM latest
    WHERE ends_on >= TO_DATE('2023-10-16')
      AND is_bundle <= 30
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(is_bundle) OVER w_running AS running_sum_is_bundle,
        MAX(is_bundle) OVER w_running AS running_max_is_bundle,
        MIN(is_bundle) OVER w_running AS running_min_is_bundle,
        MAX(last_known_is_bundle_2) OVER (PARTITION BY channel) AS channel_max_last_known_is_bundle_2
    FROM valid
    WINDOW w_running AS (PARTITION BY promo_code, channel ORDER BY last_known_is_bundle_2_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    is_bundle,
    resolved_channel,
    last_known_is_bundle_2,
    last_known_is_bundle_2_rounded,
    running_max_is_bundle,
    CASE WHEN promo_code RLIKE '^(WELCOME5|VIP)$' THEN 'EXTENDED' WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'GROUP_A' ELSE 'CORE' END AS promo_code_zone
FROM with_windows
ORDER BY id DESC
LIMIT 20;
