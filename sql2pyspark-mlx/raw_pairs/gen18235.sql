WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        LOWER(promo_code) AS promo_code_lower,
        ADD_MONTHS(ends_on, 6) AS ends_on_plus_months
    FROM promotions
    WHERE starts_on BETWEEN '2023-12-05' AND '2025-01-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_code_lower,
        ends_on_plus_months,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM base
    WHERE ends_on >= DATE '2023-09-10'
      AND ends_on_plus_months <= '2023-08-23'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        ends_on,
        COUNT(days_since_ends_on) OVER (PARTITION BY promo_code ORDER BY ends_on_plus_months, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_days_since_ends_on,
        MAX(days_since_ends_on) OVER (PARTITION BY promo_code ORDER BY ends_on_plus_months, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_days_since_ends_on,
        SUM(days_since_ends_on) OVER (PARTITION BY promo_code ORDER BY ends_on_plus_months, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_days_since_ends_on
    FROM filtered
    WHERE starts_on >= '2024-03-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(last3_max_days_since_ends_on) OVER (PARTITION BY promo_code) AS promo_code_count_last3_max_days_since_ends_on,
        FIRST_VALUE(last3_sum_days_since_ends_on, TRUE) OVER (PARTITION BY promo_code ORDER BY last3_sum_days_since_ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_sum_days_since_ends_on,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY last3_count_days_since_ends_on DESC, id) AS last3_count_days_since_ends_on_bucket
    FROM enriched
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY last3_count_days_since_ends_on_bucket DESC NULLS LAST) AS last3_count_days_since_ends_on_bucket_percent_rank,
        SUM(last3_max_days_since_ends_on) OVER w_part AS cumulative_last3_max_days_since_ends_on,
        NTILE(10) OVER (ORDER BY first_known_last3_sum_days_since_ends_on, id) AS first_known_last3_sum_days_since_ends_on_bucket
    FROM metrics
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_code_count_last3_max_days_since_ends_on DESC, id) AS latest_rank
    FROM enriched_promotions
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_count_days_since_ends_on >= 16 THEN 'HIGH' ELSE 'LOW' END AS last3_count_days_since_ends_on_tier,
    promo_code,
    CASE WHEN last3_sum_days_since_ends_on >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_sum_days_since_ends_on_tier,
    SUM(last3_sum_days_since_ends_on) FILTER (WHERE promo_code = 'VIP') AS vip_total_last3_sum_days_since_ends_on,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    APPROX_PERCENTILE(last3_sum_days_since_ends_on, 0.5) AS last3_sum_days_since_ends_on_p50,
    AVG(last3_count_days_since_ends_on) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_avg_last3_count_days_since_ends_on,
    AVG(promo_code_count_last3_max_days_since_ends_on) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_promo_code_count_last3_max_days_since_ends_on,
    SUM(CASE WHEN promo_code = 'SPRING10' THEN last3_max_days_since_ends_on ELSE 0 END) AS spring10_last3_max_days_since_ends_on
FROM deduped
GROUP BY CUBE (CASE WHEN last3_count_days_since_ends_on >= 16 THEN 'HIGH' ELSE 'LOW' END, promo_code, CASE WHEN last3_sum_days_since_ends_on >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY promo_code DESC
