SELECT *, DATE_TRUNC('week', sale_date) AS sale_date_period
FROM store_sales
WHERE product_id IN (1, 6) AND product_id = 7;
