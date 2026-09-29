WITH base AS (
    SELECT *
    FROM subscriptions
    WHERE end_date > '2023-09-01' OR customer_id IN (12, 18, 27)
),
Agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM base
    GROUP BY status
)
SELECT *
FROM Agg
ORDER BY status ASC;
