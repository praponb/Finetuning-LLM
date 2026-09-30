SELECT *
FROM order_items
WHERE unit_price IS NOT NULL AND product_id IS NOT NULL;
