SELECT
    id,
    product_id,
    units,
    LAST_DAY(sale_date) sale_date_month_end
FROM store_sales
WHERE sale_date < '2025-08-01' OR store_id IN (1, 3, 4, 5)
