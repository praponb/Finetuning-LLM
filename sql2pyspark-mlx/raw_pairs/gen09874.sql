WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        status,
        UPPER(status) status_upper,
        COALESCE(method, 'NONE') resolved_method
    FROM payments
    WHERE amount BETWEEN 730 AND 830
),
base_payments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(method) clean_method,
        LENGTH(method) method_length
    FROM base
    WHERE method = 'card'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(method_length, 0) method_length_rounded,
        COALESCE(method, 'N/A') method_clean
    FROM base_payments
    WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED')
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.clean_method,
        s.method_length,
        s.method_length_rounded,
        p.order_date
    FROM cleaned s
    LEFT JOIN orders p
        ON s.order_id = p.id
    WHERE p.order_date BETWEEN '2024-04-06' AND '2024-08-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        clean_method,
        method_length,
        method_length_rounded,
        COUNT(method_length) OVER (PARTITION BY method ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_method_length,
        SUM(method_length) OVER (PARTITION BY method ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_method_length,
        MIN(method_length) OVER (PARTITION BY method ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_method_length
    FROM with_dims
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.clean_method,
        s.method_length,
        s.method_length_rounded,
        s.running_count_method_length,
        d.amount,
        d.discount
    FROM metrics s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.shipping_fee,
        d.status
    FROM joined a
    JOIN orders d
        ON a.order_id = d.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN shipping_fee >= 34 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    CASE WHEN method_length >= 68 THEN 'LARGE' ELSE 'SMALL' END AS method_length_tier,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) AS shipping_fee_per_pending,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS grouping_level
FROM with_dims_payments
GROUP BY GROUPING SETS ((CASE WHEN shipping_fee >= 34 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN method_length >= 68 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN shipping_fee >= 34 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY distinct_ids
