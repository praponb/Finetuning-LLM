SELECT
    c.city,
    c.name,
    t.num_sessions,
    t.browser_list
FROM customers c
INNER JOIN (SELECT customer_id, COUNT(*) num_sessions, SORT_ARRAY(COLLECT_SET(browser)) browser_list
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.num_sessions >= 5
ORDER BY city DESC;
