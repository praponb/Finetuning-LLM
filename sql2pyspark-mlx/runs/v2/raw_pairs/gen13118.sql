SELECT c.id, c.city, t.country_list
FROM customers c
INNER JOIN (SELECT customer_id, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM sessions
    WHERE device IN ('tablet', 'desktop', 'mobile')
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY c.city DESC, id DESC;
