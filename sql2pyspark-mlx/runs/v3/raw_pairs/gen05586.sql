SELECT c.age, t.safari_count, t.browser_values
FROM customers c
INNER JOIN (SELECT customer_id, COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(browser))) AS browser_values
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.safari_count > 9
ORDER BY age DESC;
