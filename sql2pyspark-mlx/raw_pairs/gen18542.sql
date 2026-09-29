SELECT
    o.shipping_fee,
    o.customer_id,
    o.order_date,
    s.opened_date
FROM orders o
JOIN stores s ON o.store_id = s.id
ORDER BY customer_id DESC;
