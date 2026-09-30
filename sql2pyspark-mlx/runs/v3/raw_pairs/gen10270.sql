SELECT *, LEAST(unit_price, quantity) AS least_unit_price_quantity
FROM order_items
WHERE unit_price <= 400
ORDER BY id DESC, product_id DESC
