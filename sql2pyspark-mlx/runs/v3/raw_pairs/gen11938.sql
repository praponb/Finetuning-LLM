WITH src AS (
    SELECT *
    FROM shipments
    WHERE carrier NOT IN ('Flash', 'FedEx')
),
Agg AS (
    SELECT carrier, MIN(shipped_date) AS first_shipped_date
    FROM src
    GROUP BY carrier
)
SELECT *
FROM Agg
ORDER BY first_shipped_date, carrier
LIMIT 5
