WITH sessions_by_customer AS (
    SELECT customer_id, COUNT(*) total_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values
    FROM sessions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.age,
    sbc.total_count,
    sbc.country_values
FROM customers c
LEFT JOIN sessions_by_customer sbc ON c.id = sbc.customer_id
WHERE sbc.total_count >= 4;
