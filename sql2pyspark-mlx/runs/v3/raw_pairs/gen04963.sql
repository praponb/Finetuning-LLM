SELECT
    store_id,
    sale_quarter,
    last_sale_date,
    avg_revenue
FROM (
    SELECT
        store_id,
        QUARTER(sale_date) AS sale_quarter,
        MAX(sale_date) AS last_sale_date,
        AVG(revenue) AS avg_revenue
    FROM store_sales
    WHERE revenue > 3500
    GROUP BY store_id, QUARTER(sale_date)
) AS t
WHERE avg_revenue < 1500
ORDER BY last_sale_date DESC
