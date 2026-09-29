WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_TRUNC('week', ends_on) AS ends_on_period,
        LAST_DAY(ends_on) AS ends_on_month_end
    FROM promotions
    WHERE discount_pct < 0.29
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY ends_on_month_end NULLS FIRST) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
latest_promotions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        ends_on_period,
        ends_on_month_end,
        row_num,
        rn,
        LENGTH(promo_code) AS promo_code_length
    FROM latest_promotions
    WHERE starts_on BETWEEN '2023-05-01' AND '2024-06-24'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(ends_on_period) OVER (PARTITION BY promo_code ORDER BY ends_on_period, id) AS prev_ends_on_period
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_ends_on_period IS NULL OR ends_on_period > prev_ends_on_period + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY promo_code ORDER BY ends_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    promo_code,
    DATE_TRUNC('MONTH', ends_on_month_end) AS activity_month,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    AVG(discount_pct) FILTER (WHERE promo_code = 'VIP') AS vip_avg_discount_pct,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    COUNT(*) AS row_count
FROM sessionized
GROUP BY promo_code, DATE_TRUNC('MONTH', ends_on_month_end)
ORDER BY promo_code
