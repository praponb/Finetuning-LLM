SELECT CASE WHEN c.credit_limit >= 17400 THEN 'high' WHEN c.credit_limit >= 6100 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, c.country, SUM(CASE WHEN s.browser = 'Chrome' THEN 1 ELSE 0 END) AS chrome_count
FROM sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 17400 THEN 'high' WHEN c.credit_limit >= 6100 THEN 'mid' ELSE 'low' END, c.country
HAVING chrome_count > 70
