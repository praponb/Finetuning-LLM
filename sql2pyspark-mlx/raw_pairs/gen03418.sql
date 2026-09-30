SELECT
    id,
    quantity,
    unit_price,
    LEAST(unit_price, quantity) AS least_unit_price_quantity
FROM order_items
WHERE quantity BETWEEN 5 AND 7
ORDER BY least_unit_price_quantity, id
LIMIT 3;
