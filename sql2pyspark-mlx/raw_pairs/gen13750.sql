SELECT name, DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date, COALESCE(segment, 'unassigned') AS segment_filled
FROM customers
WHERE (signup_date > '2025-01-07' AND LENGTH(email) > 7) OR signup_date BETWEEN '2024-06-16' AND '2024-09-01'
ORDER BY segment_filled
