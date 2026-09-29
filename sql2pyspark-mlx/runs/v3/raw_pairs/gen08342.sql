SELECT *
FROM (
    SELECT
        product_id,
        store_id,
        AVG(revenue) avg_revenue,
        MIN(revenue) min_revenue
    FROM store_sales
    WHERE units > 34
    GROUP BY product_id, store_id
) s
WHERE min_revenue < 1900
ORDER BY product_id;
