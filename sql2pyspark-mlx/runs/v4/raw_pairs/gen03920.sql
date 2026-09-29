WITH base AS (
    SELECT *, DAYOFWEEK(delivered_date) AS delivered_date_dayofweek
    FROM shipments
    WHERE NOT (cost < 35 OR cost BETWEEN 38 AND 59)
),
carrier_summary AS (
    SELECT
        carrier,
        MONTH(shipped_date) AS shipped_month,
        COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
        ROUND(AVG(cost), 2) AS avg_cost
    FROM base
    GROUP BY carrier, MONTH(shipped_date)
)
SELECT carrier, fedex_count, avg_cost
FROM carrier_summary
