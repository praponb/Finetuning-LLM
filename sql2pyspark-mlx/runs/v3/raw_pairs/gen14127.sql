SELECT *, CAST(quantity AS STRING) AS quantity_num
FROM order_items
WHERE unit_price <= 300
ORDER BY unit_price DESC, id
LIMIT 20
