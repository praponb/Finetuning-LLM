SELECT
    id,
    product_id,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
    DATE_ADD(sale_date, 76) AS sale_date_plus_76d
FROM store_sales
WHERE units != 20 AND (revenue BETWEEN 2300 AND 3700 OR store_id IN (2, 3, 4))
ORDER BY id DESC;
