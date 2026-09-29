SELECT id, quantity, ROUND(quantity, 2) quantity_rounded
FROM order_items
WHERE quantity < 6 OR quantity <= 4
ORDER BY quantity DESC, id DESC;
