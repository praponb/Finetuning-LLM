SELECT
    c.city,
    c.country,
    o.discount,
    o.order_date,
    o.amount
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id;
