SELECT
    id,
    units,
    DAYOFWEEK(sale_date) AS sale_date_dayofweek,
    LAST_DAY(sale_date) AS sale_date_month_end
FROM store_sales
WHERE sale_date BETWEEN '2024-09-22' AND '2025-02-01'
ORDER BY id DESC
LIMIT 10
