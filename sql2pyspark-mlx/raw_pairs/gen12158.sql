SELECT
    c.country,
    c.city,
    MAX(p.amount) - MIN(p.amount) AS amount_range,
    SUM(CASE WHEN p.method = 'card' THEN p.amount ELSE 0 END) AS card_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY c.country, c.city
HAVING card_amount > 47
