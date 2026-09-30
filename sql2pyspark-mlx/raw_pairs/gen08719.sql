WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-08-30'
      AND store_id = 6
)
-- Window metrics over several different partitions
SELECT
    id,
    product_id,
    quantity_filled,
    NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS quantity_bucket,
    AVG(quantity_filled) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_filled_avg_6_prior,
    SUM(quantity_filled) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled
FROM cleaned;
