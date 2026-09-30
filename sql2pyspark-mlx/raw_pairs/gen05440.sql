WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist,
        SUM(amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
        LAG(amount, 2) OVER (PARTITION BY status, method ORDER BY amount, id) AS prev_amount
    FROM payments
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_sum_30d DESC, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.amount,
        s.status,
        s.running_amount,
        s.prev_amount,
        b.order_date,
        b.status AS order_status
    FROM deduped s
    INNER JOIN orders b
        ON s.order_id = b.id
    WHERE b.store_id = 30
),
deduped_payments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY running_amount DESC NULLS LAST, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_amount, id) AS row_num_2
    FROM deduped_payments
    QUALIFY row_num_2 = 1
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.status,
    s.prev_amount,
    s.order_status,
    s.row_num_2,
    CASE WHEN order_status RLIKE '^(RETURNED|PENDING)$' THEN 'CORE' ELSE 'EXTENDED' END AS order_status_zone,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM ranked s
