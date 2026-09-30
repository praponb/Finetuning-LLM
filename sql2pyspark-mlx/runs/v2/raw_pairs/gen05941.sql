SELECT * FROM (SELECT status, ROUND(AVG(discount), 2) AS avg_discount FROM orders WHERE channel IN ('partner', 'mobile', 'web') GROUP BY status) AS sub WHERE avg_discount >= 28 ORDER BY status DESC
