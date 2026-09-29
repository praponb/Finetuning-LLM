WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
base AS (
    SELECT *, HOUR(created_at) AS created_at_hour
    FROM exploded
    WHERE created_at >= DATE '2025-03-01'
      AND tag IN ('BUNDLE', 'VIP')
),
metrics AS (
    SELECT
        *,
        MAX(created_at_hour) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_hour_max_2h,
        SUM(created_at_hour) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_hour_sum_2h,
        MAX(created_at_hour) OVER (PARTITION BY customer_id, tag) AS customer_id_max_created_at_hour,
        STDDEV_SAMP(created_at_hour) OVER (PARTITION BY MONTH(created_at), customer_id) AS created_at_month_stddev_samp_created_at_hour,
        DENSE_RANK() OVER (ORDER BY created_at_hour) AS created_at_hour_dense_rank
    FROM base
    WHERE tag = 'VIP'
),
base_carts AS (
    SELECT
        id,
        customer_id,
        created_at,
        created_at_hour,
        created_at_hour_sum_2h,
        customer_id_max_created_at_hour,
        created_at_month_stddev_samp_created_at_hour,
        customer_id_max_created_at_hour / NULLIF(created_at_month_stddev_samp_created_at_hour, 0) AS customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour,
        created_at_hour_sum_2h - created_at_hour_dense_rank AS created_at_hour_sum_2h_minus_created_at_hour_dense_rank
    FROM metrics
    WHERE created_at_month_stddev_samp_created_at_hour < 52
      AND tag IS NOT NULL
),
metrics_carts AS (
    SELECT
        *,
        COUNT(created_at_hour_sum_2h_minus_created_at_hour_dense_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_hour_sum_2h_minus_created_at_hour_dense_rank_count_1h,
        COUNT(customer_id_max_created_at_hour) OVER (PARTITION BY customer_id) AS customer_id_count_customer_id_max_created_at_hour,
        MAX(created_at_hour) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_hour_max_1h,
        STDDEV_SAMP(created_at_hour_sum_2h) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_created_at_hour_sum_2h,
        AVG(created_at_hour_sum_2h) OVER (PARTITION BY TO_DATE(created_at), customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_hour_sum_2h_avg_15m
    FROM base_carts
),
enriched AS (
    SELECT
        customer_id,
        created_at_hour,
        created_at_hour_sum_2h,
        customer_id_max_created_at_hour,
        created_at_month_stddev_samp_created_at_hour,
        customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour,
        created_at_hour_sum_2h_minus_created_at_hour_dense_rank,
        customer_id_count_customer_id_max_created_at_hour,
        created_at_hour_max_1h,
        customer_id_stddev_samp_created_at_hour_sum_2h,
        created_at_hour_sum_2h_avg_15m,
        ROUND(customer_id_count_customer_id_max_created_at_hour / SUM(customer_id_count_customer_id_max_created_at_hour) OVER (PARTITION BY customer_id), 4) AS customer_id_count_customer_id_max_created_at_hour_share,
        SUM(customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour_sum_1h
    FROM metrics_carts
),
summary AS (
    SELECT
        CASE WHEN created_at_hour >= 73 THEN 'HIGH' ELSE 'LOW' END AS created_at_hour_tier,
        AVG(created_at_hour_sum_2h) AS avg_created_at_hour_sum_2h,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids
    FROM enriched
    GROUP BY 1
    HAVING COUNT(DISTINCT customer_id) < 1000
)
SELECT created_at_hour_tier, avg_created_at_hour_sum_2h
FROM summary
ORDER BY created_at_hour_tier DESC
