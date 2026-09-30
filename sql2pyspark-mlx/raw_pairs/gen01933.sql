WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(ends_on, 81) AS ends_on_minus_81d,
        LAST_DAY(ends_on) AS ends_on_month_end,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel NOT IN ('store', 'mobile')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on DESC) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
latest_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on_minus_81d, id) = 1
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    ends_on_minus_81d,
    ends_on_month_end,
    channel_filled,
    row_num,
    CASE WHEN channel RLIKE '^(mobile|partner)$' THEN 'EXTENDED' WHEN channel RLIKE '^(web|store)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS channel_zone
FROM latest_promotions
ORDER BY ends_on_minus_81d DESC;
