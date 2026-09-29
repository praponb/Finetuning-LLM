WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        entry.product_id,
        entry.qty
    FROM carts src
    LATERAL VIEW EXPLODE(src.items) t AS entry
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        qty,
        value
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        created_at,
        qty,
        STDDEV_SAMP(qty) OVER w_recent AS created_at_month_stddev_samp_qty,
        COUNT(qty) OVER (PARTITION BY customer_id) AS customer_id_count_qty,
        COUNT(qty) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_count_2h,
        SUM(qty) OVER (PARTITION BY customer_id, value) AS customer_id_sum_qty,
        AVG(qty) OVER w_recent_2 AS qty_avg_1h
    FROM flattened_carts
    WINDOW w_recent AS (PARTITION BY MONTH(created_at), value), w_recent_2 AS (PARTITION BY DATE_TRUNC('month', created_at), value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        created_at,
        created_at_month_stddev_samp_qty,
        customer_id_count_qty,
        qty_count_2h,
        COUNT(customer_id_sum_qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS customer_id_sum_qty_count_15m,
        PERCENT_RANK() OVER (PARTITION BY MONTH(created_at), customer_id ORDER BY qty_count_2h DESC) AS qty_count_2h_percent_rank,
        CUME_DIST() OVER (ORDER BY customer_id_count_qty DESC) AS customer_id_count_qty_cume_dist,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY customer_id_count_qty DESC NULLS FIRST) AS customer_id_count_qty_dense_rank
    FROM scored
    WHERE qty_avg_1h > 3
)
SELECT
    created_at,
    created_at_month_stddev_samp_qty,
    customer_id_count_qty,
    qty_count_2h,
    customer_id_sum_qty_count_15m,
    qty_count_2h_percent_rank,
    customer_id_count_qty_cume_dist,
    customer_id_count_qty_dense_rank
FROM enriched
ORDER BY qty_count_2h_percent_rank DESC, created_at DESC
