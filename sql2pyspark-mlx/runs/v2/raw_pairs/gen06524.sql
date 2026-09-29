SELECT is_active, MAX(age) - MIN(age) AS age_range
FROM customers
WHERE LENGTH(email) >= 8 AND city IS NOT NULL
GROUP BY is_active;
