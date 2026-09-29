SELECT carrier, unique_orders
FROM (
    SELECT carrier, COUNT(DISTINCT cost) AS unique_orders
    FROM shipments
    WHERE order_id IS NOT NULL
    GROUP BY carrier
) AS agg
WHERE unique_orders >= 5
