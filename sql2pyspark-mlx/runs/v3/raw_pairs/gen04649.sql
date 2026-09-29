SELECT
    id,
    units,
    product_id,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales;
