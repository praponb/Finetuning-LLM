SELECT id, product_id, unit_price, ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 3) AS unit_price_share FROM order_items WHERE unit_price <= 390 ORDER BY unit_price_share DESC
