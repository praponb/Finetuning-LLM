SELECT id, store_id, ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales
WHERE units > 13 AND (product_id IN (2, 3, 6, 10) OR sale_date > '2023-03-30');
