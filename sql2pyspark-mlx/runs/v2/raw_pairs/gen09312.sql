SELECT
    s.opened_date,
    o.customer_id,
    o.order_date,
    o.shipping_fee
FROM stores s
LEFT OUTER JOIN orders o ON s.id = o.store_id
ORDER BY customer_id DESC
