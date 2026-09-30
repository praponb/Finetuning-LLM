SELECT
    o.id,
    o.store_id,
    o.customer_id,
    s.store_name,
    o.shipping_fee + o.amount AS amount_with_shipping
FROM orders o
LEFT OUTER JOIN stores s ON o.store_id = s.id
ORDER BY s.store_name, o.id DESC;
