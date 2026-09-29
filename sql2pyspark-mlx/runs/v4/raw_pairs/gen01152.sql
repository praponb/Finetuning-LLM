SELECT o.channel, c.city, c.is_active
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.segment IN ('SMB', 'Corporate')
ORDER BY channel;
