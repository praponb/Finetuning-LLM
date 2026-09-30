SELECT
    stock_qty,
    brand,
    TRIM(brand) AS clean_brand,
    DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
FROM products
ORDER BY days_since_launch_date, brand;
