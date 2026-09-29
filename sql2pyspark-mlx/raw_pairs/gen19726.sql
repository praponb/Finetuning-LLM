WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-05-14 13:00:00'
      AND method IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM valid
    EXCEPT ALL
    SELECT status
    FROM payments
    WHERE method IN ('promptpay', 'card', 'cash')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.status = k.status
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.status AS order_status,
        p.channel,
        p.amount AS order_amount
    FROM kept_rows e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_filled,
        method_clean,
        order_status,
        channel,
        order_amount,
        CONCAT_WS('-', channel, status) AS channel_status_key,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM joined
    WHERE paid_at > TO_DATE('2023-06-17')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        method_clean,
        order_status,
        order_amount,
        channel_status_key,
        INITCAP(TRIM(method)) AS method_title
    FROM filtered
    WHERE order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.status AS order_status_2,
        b.order_date
    FROM cleaned src
    LEFT JOIN orders b
        ON src.order_id <=> b.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.amount,
        s.paid_at,
        s.method_clean,
        s.order_status,
        s.method_title,
        p.amount AS order_amount_2,
        p.status AS order_status_3,
        p.channel
    FROM with_dims s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN order_amount_2 >= 1280 THEN 'PREMIUM' ELSE 'STANDARD' END AS order_amount_2_tier,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    MAX(paid_at) AS last_seen,
    COUNT(*) FILTER (WHERE order_status = 'COMPLETED') AS completed_rows,
    PERCENTILE_APPROX(order_amount_2, 0.9) AS order_amount_2_p90,
    GROUPING(DATE_TRUNC('YEAR', paid_at)) AS gid
FROM with_dims_payments
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), CASE WHEN order_amount_2 >= 1280 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('YEAR', paid_at)))
ORDER BY order_amount_2_p90
