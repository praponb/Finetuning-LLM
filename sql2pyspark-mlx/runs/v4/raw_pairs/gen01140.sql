SELECT
    c.id,
    c.segment,
    t.total_count,
    t.browser_values
FROM customers c
LEFT JOIN (SELECT customer_id, COUNT(*) AS total_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(browser))) AS browser_values
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.total_count >= 7
ORDER BY total_count DESC;
