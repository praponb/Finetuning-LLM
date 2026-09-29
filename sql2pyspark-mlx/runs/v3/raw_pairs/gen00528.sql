WITH recent_shipments AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM shipments
    WHERE order_id IN (15, 22, 25, 28)
),
carrier_summary AS (
    SELECT carrier, COUNT(*) AS num_shipments
    FROM recent_shipments
    GROUP BY carrier
)
SELECT carrier, num_shipments
FROM carrier_summary
WHERE num_shipments > 10
ORDER BY carrier ASC, num_shipments DESC
LIMIT 5;
