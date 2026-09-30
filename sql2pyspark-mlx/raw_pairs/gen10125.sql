WITH customer_summary AS (
    SELECT customer_id, COUNT(DISTINCT country) AS unique_countrys, SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
    FROM sessions
    WHERE device IN ('mobile', 'tablet', 'desktop')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.is_active,
    cs.unique_countrys,
    cs.browser_list
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.unique_countrys > 7
ORDER BY id ASC;
