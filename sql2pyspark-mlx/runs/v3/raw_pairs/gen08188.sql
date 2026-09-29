SELECT *
FROM order_items
WHERE quantity <> 7
ORDER BY unit_price DESC, id
LIMIT 10;
