WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, attr_value
),
base AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value,
        CASE WHEN attr_key = 'device' THEN 1 ELSE 0 END AS is_device
    FROM flattened
    WHERE attr_key IS NOT NULL
),
metrics AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value,
        is_device,
        PERCENT_RANK() OVER (ORDER BY is_device NULLS FIRST) AS is_device_percent_rank,
        MAX(is_device) OVER (PARTITION BY attr_key, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_device_max_1h
    FROM base
),
enriched AS (
    SELECT
        *,
        SUM(is_device) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_device_sum_1h,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY is_device_percent_rank DESC NULLS LAST) AS is_device_percent_rank_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY is_device_percent_rank) AS is_device_percent_rank_percent_rank
    FROM metrics
),
enriched_carts AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value,
        is_device,
        is_device_percent_rank,
        is_device_max_1h,
        is_device_sum_1h,
        is_device_percent_rank_cume_dist,
        is_device_percent_rank_percent_rank,
        COUNT(is_device_percent_rank_percent_rank) OVER (PARTITION BY customer_id) AS customer_id_count_is_device_percent_rank_percent_rank,
        AVG(is_device) OVER (PARTITION BY customer_id, attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_device_avg_2h
    FROM enriched
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY attr_key, customer_id ORDER BY is_device_percent_rank_cume_dist DESC) AS is_device_percent_rank_cume_dist_percent_rank,
        COUNT(is_device_sum_1h) OVER (PARTITION BY customer_id, attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_device_sum_1h_count_1h,
        MIN(customer_id_count_is_device_percent_rank_percent_rank) OVER (PARTITION BY attr_key) AS attr_key_min_customer_id_count_is_device_percent_rank_percent_rank
    FROM enriched_carts
)
SELECT
    id,
    created_at,
    attr_value,
    is_device,
    is_device_percent_rank,
    is_device_max_1h,
    is_device_sum_1h,
    is_device_percent_rank_cume_dist,
    customer_id_count_is_device_percent_rank_percent_rank
FROM with_windows
ORDER BY is_device DESC NULLS LAST, id DESC;
