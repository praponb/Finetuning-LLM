SELECT
    o.id,
    o.shipping_fee,
    o.amount,
    c.country,
    c.credit_limit,
    s.opened_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
ORDER BY amount DESC, id
LIMIT 5;
