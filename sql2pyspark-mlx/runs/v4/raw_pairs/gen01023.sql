WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY method, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount NULLS LAST) AS amount_dense_rank
    FROM payments
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC NULLS LAST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.status AS order_status,
        d.discount
    FROM ranked a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT method, order_id
    FROM enriched
    EXCEPT ALL
    SELECT method, order_id
    FROM payments
    WHERE TO_DATE(paid_at) >= '2024-04-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.method = k.method AND s.order_id = k.order_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM kept_rows
    EXCEPT
    SELECT order_id, status
    FROM payments
    WHERE method IN ('promptpay', 'paypal')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.status = k.status
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        last_known_method,
        amount_dense_rank,
        latest_rank,
        order_status,
        discount,
        SUM(discount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS discount_sum_30d,
        COUNT(amount) OVER w_ordered AS amount_count_90d
    FROM reconciled
    WINDOW w_ordered AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN amount_count_90d >= 14 THEN 'HIGH' ELSE 'LOW' END AS amount_count_90d_tier,
    ROUND(SUM(discount_sum_30d) / NULLIF(COUNT(CASE WHEN method = 'cash' THEN 1 END), 0), 2) AS discount_sum_30d_per_cash,
    AVG(discount) FILTER (WHERE order_status = 'COMPLETED') AS completed_avg_discount,
    MIN(paid_at) AS started_at,
    SIZE(COLLECT_SET(order_status)) AS num_order_status
FROM metrics
GROUP BY CASE WHEN amount_count_90d >= 14 THEN 'HIGH' ELSE 'LOW' END
ORDER BY amount_count_90d_tier DESC;
