SELECT
    id,
    store_id,
    revenue - units AS revenue_per_unit,
    COALESCE(units, 0) AS units_filled
FROM store_sales
WHERE product_id IN (1, 5, 6, 8) OR units IS NULL
ORDER BY store_id DESC;
