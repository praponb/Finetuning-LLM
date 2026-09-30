WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', status, carrier) AS status_carrier_key,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE shipped_date <= '2025-09-01'
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.cost,
        src.status,
        src.status_carrier_key,
        src.carrier_filled,
        b.amount,
        b.order_date
    FROM filtered src
    LEFT JOIN orders b
        ON src.order_id = b.id
    WHERE b.channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost) OVER w_running AS prev_cost,
        LEAD(cost) OVER w_running AS next_cost,
        LEAD(amount) OVER w_running AS next_amount,
        LAG(amount) OVER w_running AS prev_amount,
        ROW_NUMBER() OVER w_running AS seq_no,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM joined
    WINDOW w_running AS (PARTITION BY order_id, status ORDER BY amount, id)
),
joined_shipments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.amount,
        src.prev_cost,
        src.seq_no,
        o.status AS order_status,
        o.discount
    FROM with_windows src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.discount AS order_discount,
        d.order_date
    FROM joined_shipments e
    LEFT JOIN orders d
        ON e.order_id = d.id
)
SELECT
    id,
    carrier,
    amount,
    prev_cost,
    seq_no,
    order_discount
FROM enriched
ORDER BY order_discount, id DESC;
