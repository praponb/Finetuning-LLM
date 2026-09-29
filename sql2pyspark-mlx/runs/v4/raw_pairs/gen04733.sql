SELECT *, ROUND(quantity / unit_price, 2) AS line_total, ROUND(unit_price, 2) AS unit_price_rounded FROM order_items WHERE quantity IS NOT NULL ORDER BY unit_price;
