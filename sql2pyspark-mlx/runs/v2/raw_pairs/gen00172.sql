SELECT
    id,
    unit_price,
    ROUND(unit_price, 0) AS unit_price_rounded,
    ROUND(quantity * 1.1, 2) AS quantity_scaled
FROM order_items
WHERE (unit_price BETWEEN 130 AND 140 AND unit_price > 190) OR quantity <= 8;
