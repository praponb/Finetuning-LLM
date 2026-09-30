WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (3, 6, 8, 10)
),
agg AS (
    SELECT
        product_id,
        MONTH(last_restock) last_restock_month,
        MAX(quantity) max_quantity,
        MIN(last_restock) first_last_restock
    FROM valid_inventory
    GROUP BY product_id, MONTH(last_restock)
)
SELECT product_id, max_quantity, first_last_restock
FROM agg
WHERE max_quantity >= 110;
