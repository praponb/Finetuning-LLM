SELECT
    id,
    store_id,
    units,
    LAST_DAY(sale_date) AS sale_date_month_end
FROM store_sales
WHERE store_id IN (1, 2, 3) AND store_id = 3
ORDER BY id DESC
