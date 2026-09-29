SELECT
    id,
    sale_date,
    store_id,
    DATE_TRUNC('WEEK', sale_date) AS sale_date_period,
    DATE_ADD(sale_date, 78) AS sale_date_plus_78d
FROM store_sales
WHERE units < 39
