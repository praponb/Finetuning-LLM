WITH valid AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE order_id = 39
),
valid_payments AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        method_clean,
        CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM valid
    WHERE paid_at >= TIMESTAMP '2024-01-15 21:00:00'
      AND order_id IS NOT NULL
),
filtered AS (
    SELECT
        id,
        paid_at,
        status,
        is_refunded,
        status_prefix,
        DATE_TRUNC('MONTH', paid_at) AS paid_at_period
    FROM valid_payments
    WHERE paid_at >= TO_TIMESTAMP('2023-12-02 01:00:00')
      AND order_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_refunded DESC, id ASC) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    paid_at,
    is_refunded,
    latest_rank
FROM ranked
ORDER BY latest_rank DESC;
