WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        CONCAT(carrier, ' - ', status) AS carrier_status,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE order_id IS NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.status AS order_status,
        o.amount
    FROM cleaned src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.amount < 640
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        cost,
        amount,
        cost * amount AS cost_times_amount,
        COALESCE(order_status, 'UNKNOWN') AS resolved_order_status
    FROM enriched
    WHERE order_status IS NOT DISTINCT FROM 'CANCELLED'
      AND delivered_date > DATE '2023-09-20'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN amount >= 1310 THEN 'High' WHEN amount >= 720 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM valid
    WHERE cost_times_amount >= 24
)
SELECT
    s.id,
    s.order_id,
    s.cost,
    s.amount,
    s.resolved_order_status,
    s.amount_band,
    (SELECT MIN(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS min_shipment_cost
FROM base s
ORDER BY id DESC
LIMIT 20;
