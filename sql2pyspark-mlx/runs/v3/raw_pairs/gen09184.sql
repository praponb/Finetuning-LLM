SELECT
    store_id,
    ROUND(STDDEV(units), 2) AS stddev_units,
    MAX(revenue) AS max_revenue,
    SUM(revenue) AS total_revenue
FROM store_sales
WHERE sale_date BETWEEN '2024-02-01' AND '2025-03-01' AND store_id IN (2, 5)
GROUP BY store_id;
