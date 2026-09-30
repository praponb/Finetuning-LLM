SELECT
    *,
    MIN(units) OVER (PARTITION BY store_id) store_id_min_units,
    LAG(units, 2) OVER (PARTITION BY store_id ORDER BY units, id) prev_units
FROM store_sales
