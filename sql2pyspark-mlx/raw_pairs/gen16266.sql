SELECT
    product_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    ROUND(SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (), 2) AS pct_of_total
FROM store_sales
GROUP BY product_id
ORDER BY product_id DESC
