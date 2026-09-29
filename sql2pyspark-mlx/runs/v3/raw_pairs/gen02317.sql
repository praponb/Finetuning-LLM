WITH CustomerTotals AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(device)) AS device_list, SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
    FROM sessions
    WHERE TO_DATE(started_at) < '2023-06-07'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.segment,
    c.city,
    C.device_list,
    C.browser_list
FROM customers c
INNER JOIN CustomerTotals C ON c.id = C.customer_id
ORDER BY c.city, segment DESC
