SELECT *
FROM order_items
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) = 1
