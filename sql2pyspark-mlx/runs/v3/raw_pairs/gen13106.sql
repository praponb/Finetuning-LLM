SELECT
    store_id,
    sale_month,
    max_revenue,
    total_revenue
FROM (
    SELECT
        store_id,
        DATE_TRUNC('MONTH', sale_date) AS sale_month,
        MAX(revenue) AS max_revenue,
        SUM(revenue) AS total_revenue
    FROM store_sales
    GROUP BY store_id, DATE_TRUNC('MONTH', sale_date)
) AS x
WHERE total_revenue >= 10000
ORDER BY store_id DESC, sale_month;
