SELECT
    id,
    revenue,
    store_id,
    units
FROM store_sales
WHERE revenue > (SELECT MAX(revenue) FROM store_sales)
ORDER BY id DESC, units DESC
