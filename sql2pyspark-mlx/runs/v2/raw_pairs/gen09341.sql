WITH src AS (
    SELECT *, LENGTH(status) AS status_length
    FROM orders
    WHERE amount >= 1000
),
status_summary AS (
    SELECT status, customer_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM src
    GROUP BY status, customer_id
)
SELECT *
FROM status_summary
ORDER BY customer_id DESC;
