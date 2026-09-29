SELECT
    order_id,
    quantity,
    quantity - unit_price line_total,
    ROUND(unit_price / quantity, 2) line_total_2
FROM order_items
WHERE unit_price BETWEEN 130 AND 340 OR product_id IN (9, 22, 34)
ORDER BY line_total ASC
