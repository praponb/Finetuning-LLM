SELECT status, MIN(discount) AS min_discount FROM orders GROUP BY status ORDER BY min_discount DESC;
