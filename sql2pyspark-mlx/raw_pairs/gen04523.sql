WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CASE WHEN cost >= 78 THEN 'High' WHEN cost >= 32 THEN 'Medium' ELSE 'Low' END AS cost_band,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-13'
      AND delivered_date <= '2023-10-27'
),
kept_keys AS (
    SELECT status, carrier
    FROM filtered
    EXCEPT
    SELECT status, carrier
    FROM shipments
    WHERE order_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.status = k.status AND s.carrier = k.carrier
),
base AS (
    SELECT *, ADD_MONTHS(delivered_date, 6) AS delivered_date_plus_months
    FROM kept_rows
    WHERE shipped_date > DATE '2023-06-21'
),
enriched AS (
    SELECT
        src.*,
        b.shipping_fee,
        b.status AS order_status,
        b.amount
    FROM base src
    LEFT JOIN orders b
        ON src.order_id = b.id
    WHERE b.status = 'PENDING'
),
base_shipments AS (
    SELECT *, ROUND(shipping_fee / cost, 2) AS shipping_fee_per_cost
    FROM enriched
    WHERE order_id IS NOT NULL
),
enriched_shipments AS (
    SELECT
        s.*,
        o.status AS order_status_2,
        o.discount,
        o.channel
    FROM base_shipments s
    JOIN orders o
        ON s.order_id = o.id
),
joined AS (
    SELECT
        e.*,
        b.status AS order_status_3,
        b.amount AS order_amount,
        b.shipping_fee AS order_shipping_fee
    FROM enriched_shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
SELECT
    d.channel AS order_channel,
    d.status AS order_status,
    COUNT(*) AS num_rows,
    SUM(src.discount) AS total_discount,
    MIN(src.shipping_fee_per_cost) AS min_shipping_fee_per_cost
FROM joined src
JOIN orders d ON src.order_id = d.id
GROUP BY d.channel, d.status
ORDER BY order_channel NULLS FIRST;
