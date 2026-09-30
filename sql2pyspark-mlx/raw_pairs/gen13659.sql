WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        TRIM(carrier) AS clean_carrier
    FROM shipments
    WHERE status <> 'RETURNED'
      AND delivered_date >= DATE '2023-07-03'
)
SELECT
    e.id,
    e.order_id,
    d.channel,
    d.discount,
    d.shipping_fee
FROM valid e
LEFT JOIN orders d
    ON e.order_id = d.id
WHERE MONTH(d.order_date) IN (1, 4, 5);
