WITH src AS (
    SELECT *
    FROM accounts
    WHERE customer_id = 30
),
Agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM src
    GROUP BY status
)
SELECT status, status_list
FROM Agg
ORDER BY status DESC;
