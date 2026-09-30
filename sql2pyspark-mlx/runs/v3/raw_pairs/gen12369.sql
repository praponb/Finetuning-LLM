SELECT COUNT(age) AS age_count, COUNT(*) AS total_count
FROM customers
WHERE is_active;
