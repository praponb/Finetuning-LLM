WITH src AS (
    SELECT *
    FROM shipments
    WHERE delivered_date <= '2023-07-01'
),
status_summary AS (
    SELECT status, MAX(cost) AS max_cost
    FROM src
    GROUP BY status
)
SELECT *
FROM status_summary
