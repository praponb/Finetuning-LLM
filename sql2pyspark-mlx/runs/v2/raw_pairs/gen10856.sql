SELECT status, MAX(discount) AS max_discount FROM orders GROUP BY 1 ORDER BY status ASC, max_discount DESC LIMIT 3;
