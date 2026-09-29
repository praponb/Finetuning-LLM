SELECT
    id,
    store_id,
    units,
    revenue,
    FIRST_VALUE(revenue) OVER (PARTITION BY store_id ORDER BY units DESC, id) AS first_revenue
FROM store_sales
WHERE revenue >= 2100;
