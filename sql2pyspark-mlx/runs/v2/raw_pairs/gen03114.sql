SELECT
    c.credit_limit,
    c.name,
    c.city,
    o.discount,
    o.shipping_fee,
    o.amount
FROM customers c
LEFT OUTER JOIN orders o ON c.id = o.customer_id
WHERE LENGTH(c.email) >= 8 AND c.country = 'SG';
