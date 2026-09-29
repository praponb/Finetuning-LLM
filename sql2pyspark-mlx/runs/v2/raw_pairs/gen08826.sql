WITH customer_totals AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM sessions
    GROUP BY customer_id
)
SELECT c.age, c.name, ct.country_values
FROM customers c
LEFT OUTER JOIN customer_totals ct ON c.id = ct.customer_id
ORDER BY 2
