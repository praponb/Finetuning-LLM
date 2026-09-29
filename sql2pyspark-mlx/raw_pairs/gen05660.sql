WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE shipped_date < '2025-08-12' AND carrier IN ('FedEx', 'Flash', 'Kerry') AND carrier != 'FedEx'
),
carrier_summary AS (
    SELECT carrier, MIN(cost) min_cost
    FROM valid_shipments
    GROUP BY carrier
)
SELECT carrier, min_cost
FROM carrier_summary
