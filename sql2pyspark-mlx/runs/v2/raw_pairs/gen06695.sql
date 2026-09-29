SELECT
    order_items.id,
    order_items.order_id,
    order_items.unit_price,
    products.supplier_id,
    products.product_name,
    orders.discount,
    orders.customer_id,
    DATEDIFF(CURRENT_DATE(), orders.order_date) AS days_since_order_date
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON orders.id = order_items.order_id
WHERE products.cost BETWEEN 490 AND 510;
