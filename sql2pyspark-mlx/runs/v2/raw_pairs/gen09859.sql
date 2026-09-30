SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM inventory
GROUP BY product_id;
