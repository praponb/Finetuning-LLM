WITH customer_totals AS (
    SELECT customer_id, SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) th_count
    FROM sessions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT c.id, c.segment, ct.th_count
FROM customers c
JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.th_count > 64
ORDER BY th_count
