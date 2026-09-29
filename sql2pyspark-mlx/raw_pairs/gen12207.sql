WITH src AS (
    SELECT *
    FROM shipments
    WHERE cost <= 28 OR carrier = 'Flash'
),
shipments_stats AS (
    SELECT MONTH(delivered_date) AS delivered_month, COUNT(DISTINCT cost) AS unique_carriers
    FROM src
    GROUP BY MONTH(delivered_date)
)
SELECT delivered_month, unique_carriers
FROM shipments_stats
ORDER BY unique_carriers ASC;
