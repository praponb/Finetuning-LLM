SELECT
    id,
    product_id,
    store_id,
    revenue,
    units,
    FIRST_VALUE(units) OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) AS first_units
FROM store_sales
