WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        ROUND(discount_pct * 0.85, 2) AS adjusted_discount_pct,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE starts_on >= '2024-03-07'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        days_since_ends_on,
        adjusted_discount_pct,
        channel_clean,
        RANK() OVER (PARTITION BY promo_code, channel ORDER BY days_since_ends_on NULLS FIRST) AS days_since_ends_on_rank,
        COUNT(adjusted_discount_pct) OVER w_running AS channel_count_adjusted_discount_pct,
        AVG(days_since_ends_on) OVER (PARTITION BY channel ORDER BY adjusted_discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_ends_on_avg_2_prior
    FROM base
    WINDOW w_running AS (PARTITION BY channel)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY days_since_ends_on_rank DESC NULLS LAST, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    channel,
    CASE WHEN days_since_ends_on >= 69 THEN 'LARGE' ELSE 'SMALL' END AS days_since_ends_on_tier,
    ROUND(AVG(days_since_ends_on_rank), 2) AS avg_days_since_ends_on_rank,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(days_since_ends_on_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS days_since_ends_on_avg_2_prior_quartiles,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM ranked
GROUP BY channel, CASE WHEN days_since_ends_on >= 69 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_days_since_ends_on_rank DESC;
