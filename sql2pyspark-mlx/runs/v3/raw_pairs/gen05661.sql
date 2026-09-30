WITH Base AS (
    SELECT *
    FROM orders
    WHERE discount >= 0.35
),
StatusSummary AS (
    SELECT status, COUNT(*) AS row_count
    FROM Base
    GROUP BY status
)
SELECT *
FROM StatusSummary
ORDER BY row_count DESC;
