SELECT *, ROUND(quantity / unit_price, 2) AS line_total, ROUND(quantity * 1.07, 2) AS adjusted_quantity FROM order_items WHERE unit_price >= 170 AND product_id IS NULL;
