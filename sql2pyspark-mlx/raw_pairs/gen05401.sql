WITH ranked AS (
    SELECT
        id,
        carrier,
        delivered_date,
        RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS rnk
    FROM shipments
    WHERE carrier IN ('Kerry', 'DHL', 'UPS')
)
SELECT id, carrier, delivered_date
FROM ranked
WHERE rnk <= 1
ORDER BY id DESC;
