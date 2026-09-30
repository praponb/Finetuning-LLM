WITH SessionsByCustomer AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-08-01'
    GROUP BY customer_id
)
SELECT c.signup_date, c.age, S.country_list
FROM customers c
INNER JOIN SessionsByCustomer S ON c.id = S.customer_id
ORDER BY age DESC;
