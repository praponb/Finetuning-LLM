SELECT *, COALESCE(credit_limit, 0) AS credit_limit_filled
FROM customers
ORDER BY segment DESC, id
LIMIT 10;
