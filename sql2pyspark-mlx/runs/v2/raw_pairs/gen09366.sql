SELECT o.channel, o.customer_id, c.credit_limit
FROM orders o
LEFT OUTER JOIN customers c ON c.id = o.customer_id
WHERE c.email NOT LIKE '%@startup.io'
ORDER BY credit_limit;
