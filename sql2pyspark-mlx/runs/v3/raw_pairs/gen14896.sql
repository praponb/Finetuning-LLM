WITH customer_totals AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 8 AND 13
    GROUP BY customer_id
)
SELECT c.id, c.is_active, ct.country_list
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
