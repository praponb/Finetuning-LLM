WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE ends_on > TO_DATE('2023-08-05')
      AND discount_pct BETWEEN 0.2 AND 0.25
),
filtered_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(starts_on, 20) AS starts_on_plus_20d,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM filtered
    WHERE ends_on > DATE '2023-10-02'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT promo_code
    FROM filtered_promotions
    INTERSECT
    SELECT promo_code
    FROM promotions
    WHERE MONTH(ends_on) IN (8, 9, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_promotions s
    JOIN kept_keys k ON s.promo_code = k.promo_code
)
SELECT
    channel,
    promo_code,
    starts_on,
    days_since_ends_on,
    resolved_channel,
    starts_on_plus_20d,
    channel_clean,
    CASE WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS promo_code_zone
FROM kept_rows
ORDER BY days_since_ends_on DESC NULLS FIRST
