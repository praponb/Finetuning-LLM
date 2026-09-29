SELECT
    store_id,
    ROUND(STDDEV(revenue), 2) stddev_revenue,
    COUNT(units) units_count,
    MIN(sale_date) min_sale_date
FROM store_sales
WHERE store_id IN (1, 2, 5)
GROUP BY store_id
HAVING stddev_revenue > 54
ORDER BY stddev_revenue DESC;
