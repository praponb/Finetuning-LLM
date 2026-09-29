SELECT *
FROM store_sales
WHERE (revenue > 3200 AND product_id IN (1, 6, 7)) OR product_id = 2
ORDER BY product_id DESC, units DESC
