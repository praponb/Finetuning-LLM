WITH kept_keys AS (
    -- Set difference on keys
    SELECT channel, id
    FROM promotions
    EXCEPT ALL
    SELECT channel, id
    FROM promotions
    WHERE starts_on < '2024-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN kept_keys k ON s.channel = k.channel AND s.id = k.id
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT channel
    FROM kept_rows
    INTERSECT ALL
    SELECT channel
    FROM promotions
    WHERE ends_on < '2025-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.channel = k.channel
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(discount_pct, 0) AS discount_pct_rounded,
        DATE_TRUNC('WEEK', ends_on) AS ends_on_period
    FROM verified
    WHERE ends_on > TO_DATE('2023-08-05')
),
filtered_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct_rounded,
        ends_on_period,
        ROUND(discount_pct_rounded, 1) AS discount_pct_rounded_rounded
    FROM filtered
    WHERE ends_on > TO_DATE('2023-09-09')
      AND MONTH(ends_on) IN (1, 2, 3)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        ends_on_period,
        discount_pct_rounded_rounded,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct_rounded_rounded NULLS FIRST) AS discount_pct_rounded_rounded_cume_dist,
        CUME_DIST() OVER (PARTITION BY YEAR(ends_on), promo_code ORDER BY discount_pct_rounded DESC NULLS FIRST) AS discount_pct_rounded_cume_dist,
        SUM(discount_pct_rounded) OVER w_part AS discount_pct_rounded_sum_7d
    FROM filtered_promotions
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY CAST(ends_on_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    channel,
    DATE_TRUNC('YEAR', ends_on_period) AS year_start,
    MIN(ends_on_period) AS first_seen,
    COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
    COUNT_IF(channel = 'store') AS store_events,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(*) AS event_count
FROM enriched
GROUP BY ROLLUP (channel, DATE_TRUNC('YEAR', ends_on_period))
HAVING SIZE(COLLECT_SET(channel)) > 1
