WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE carrier IS NOT NULL
),
carrier_summary AS (
    SELECT carrier, MIN(cost) AS min_cost, COUNT(*) AS shipment_count
    FROM recent_shipments
    GROUP BY carrier
)
SELECT carrier, min_cost, shipment_count
FROM carrier_summary
WHERE shipment_count > 5;
