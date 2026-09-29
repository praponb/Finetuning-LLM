SELECT customers.credit_limit, sessions.country, CASE WHEN customers.country = 'TH' THEN 1 ELSE 0 END is_th
FROM customers
LEFT OUTER JOIN sessions ON sessions.customer_id = customers.id
WHERE sessions.customer_id IS NULL;
