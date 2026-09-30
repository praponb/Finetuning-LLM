SELECT
    o.store_id,
    o.amount,
    o.order_date,
    s.opened_date,
    s.sqft
FROM orders o
INNER JOIN stores s ON s.id = o.store_id;
