WITH base AS (
    SELECT *
    FROM shipments
    WHERE status IN ('LOST', 'DELIVERED', 'RETURNED') AND carrier IN ('DHL', 'FedEx') AND carrier IN ('Kerry', 'FedEx', 'DHL')
),
summary AS (
    SELECT YEAR(shipped_date) AS shipped_year, carrier, COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
    FROM base
    GROUP BY YEAR(shipped_date), carrier
)
SELECT *
FROM summary
ORDER BY shipped_year DESC
