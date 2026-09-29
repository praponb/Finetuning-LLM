WITH src AS (
    SELECT *
    FROM shipments
    WHERE carrier = 'Flash'
),
Summary AS (
    SELECT QUARTER(delivered_date) delivered_quarter, COUNT(DISTINCT cost) unique_carriers, MIN(shipped_date) min_shipped_date
    FROM src
    GROUP BY QUARTER(delivered_date)
)
SELECT *
FROM Summary
WHERE unique_carriers < 8
