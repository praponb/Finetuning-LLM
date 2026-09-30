SELECT *, CAST(unit_price AS INT) AS unit_price_int
FROM order_items
WHERE order_id IN (3, 8)
ORDER BY id DESC;
