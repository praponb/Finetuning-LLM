WITH customer_totals AS (
    SELECT customer_id, COUNT(CASE WHEN country = 'TH' THEN 1 END) th_count, MAX(started_at) last_started_at
    FROM sessions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.signup_date,
    ct.th_count,
    ct.last_started_at
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.th_count > 5
ORDER BY id DESC, signup_date;
