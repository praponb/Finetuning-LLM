SELECT
    store_id,
    units_bucket,
    last_sale_date,
    revenue_count
FROM (
    SELECT
        store_id,
        CASE WHEN units >= 37 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END AS units_bucket,
        MAX(sale_date) AS last_sale_date,
        COUNT(revenue) AS revenue_count
    FROM store_sales
    WHERE units BETWEEN 28 AND 35
    GROUP BY store_id, CASE WHEN units >= 37 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END
) AS x
WHERE revenue_count < 6
ORDER BY store_id;
