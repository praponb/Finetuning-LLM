WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(method, ' - ', status) AS method_status,
        CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE paid_at > DATE '2023-11-09'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_int DESC NULLS FIRST, id) = 1
),
base_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(amount_int, 0) AS amount_int_filled,
        SUBSTRING(status, 1, 2) AS status_prefix,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM latest
    WHERE method IS NOT NULL
),
latest_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        amount,
        status,
        amount_int,
        amount_int_filled,
        resolved_method
    FROM base_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.status,
        src.amount_int,
        src.amount_int_filled,
        src.resolved_method,
        o.amount AS order_amount,
        o.discount,
        o.status AS order_status
    FROM latest_payments src
    JOIN orders o
        ON src.order_id <=> o.id
    WHERE YEAR(o.order_date) >= 2024
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(order_status) IGNORE NULLS OVER w_grp AS last_known_order_status,
        NTILE(3) OVER (ORDER BY order_amount DESC, id) AS order_amount_bucket,
        ROW_NUMBER() OVER w_running AS seq_no,
        LEAD(amount_int_filled, 2) OVER w_grp_2 AS next_amount_int_filled
    FROM joined
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id, order_status ORDER BY amount_int DESC, id), w_grp_2 AS (PARTITION BY order_id ORDER BY order_amount, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        status,
        amount_int,
        amount_int_filled,
        resolved_method,
        order_amount,
        discount,
        order_status,
        last_known_order_status,
        order_amount_bucket,
        seq_no,
        next_amount_int_filled,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM with_windows
    WHERE order_id IS NOT NULL
      AND amount_int BETWEEN 70 AND 71
)
SELECT
    s.id,
    s.order_id,
    s.status,
    s.amount_int,
    s.amount_int_filled,
    s.resolved_method,
    s.order_amount,
    s.order_status,
    s.last_known_order_status,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM filtered s
ORDER BY amount_int DESC;
