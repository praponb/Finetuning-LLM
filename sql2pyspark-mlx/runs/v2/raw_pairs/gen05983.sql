SELECT
    o.discount,
    o.order_date,
    o.amount,
    p.method
FROM orders o
JOIN payments p ON o.id = p.order_id;
