WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id = 5 AND product_id = 4
),
store_id_summary AS (
    SELECT
        store_id,
        product_id,
        SUM(quantity) AS total_quantity,
        COUNT(*) AS total_count
    FROM valid_inventory
    GROUP BY store_id, product_id
)
SELECT *
FROM store_id_summary;
