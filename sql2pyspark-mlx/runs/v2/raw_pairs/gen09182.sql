WITH customer_totals AS (
    SELECT customer_id, COUNT(DISTINCT device) AS unique_devices, COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count
    FROM sessions
    WHERE browser = 'Edge'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.country,
    ct.unique_devices,
    ct.de_count
FROM customers c
LEFT OUTER JOIN customer_totals ct ON c.id = ct.customer_id;
