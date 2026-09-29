SELECT
    c.city,
    c.country,
    c.segment,
    o.customer_id,
    o.shipping_fee,
    o.channel
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id;
