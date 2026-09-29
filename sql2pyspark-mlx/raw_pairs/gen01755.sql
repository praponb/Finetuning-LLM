SELECT
    id,
    store_id,
    product_id,
    ROUND(units, 2) AS units_rounded
FROM store_sales
WHERE NOT (revenue >= 1600 AND store_id IN (2, 5, 6) AND product_id IN (1, 7, 8, 10));
