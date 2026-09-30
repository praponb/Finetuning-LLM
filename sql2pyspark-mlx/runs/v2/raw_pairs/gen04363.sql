SELECT
    id,
    store_id,
    units,
    DATE_SUB(sale_date, 19) AS sale_date_minus_19d,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
WHERE revenue IS NULL
