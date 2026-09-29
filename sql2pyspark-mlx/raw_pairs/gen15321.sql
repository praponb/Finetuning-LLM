WITH cleaned AS (
    SELECT
        id,
        order_id,
        amount,
        CONCAT(status, ' - ', method) status_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-08-28 23:00:00'
      AND status IN ('REFUNDED', 'FAILED')
),
scored AS (
    SELECT
        id,
        order_id,
        amount,
        status_method,
        NTILE(10) OVER w_part amount_bucket,
        NTILE(3) OVER w_part amount_bucket_2,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) event_seq
    FROM cleaned
    WINDOW w_part AS (PARTITION BY order_id ORDER BY amount, id)
),
kept_keys AS (
    SELECT id
    FROM scored
    INTERSECT
    SELECT id
    FROM payments
    WHERE status IN ('SUCCESS', 'FAILED')
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
),
remaining_keys AS (
    SELECT id, order_id
    FROM kept_rows
    EXCEPT
    SELECT id, order_id
    FROM payments
    WHERE order_id = 16
),
kept_rows_payments AS (
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
enriched AS (
    SELECT
        e.id,
        e.order_id,
        e.amount,
        e.amount_bucket,
        o.discount
    FROM kept_rows_payments e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
SELECT
    CASE WHEN amount_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS amount_bucket_tier,
    order_id,
    PERCENTILE_APPROX(discount, 0.9) AS discount_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(amount) AS max_amount
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN amount_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END, order_id), (CASE WHEN amount_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING MAX(amount) > 0;
