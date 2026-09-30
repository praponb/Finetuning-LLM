WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(quantity, 0) quantity_filled
    FROM inventory
    WHERE MONTH(last_restock) IN (6, 9, 11)
)
-- Ranking and gap features
SELECT
    id,
    quantity,
    last_restock,
    MIN(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_quantity,
    COUNT(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_quantity,
    AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_quantity,
    MAX(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id) row_seq
FROM valid
WHERE quantity IS NOT NULL;
