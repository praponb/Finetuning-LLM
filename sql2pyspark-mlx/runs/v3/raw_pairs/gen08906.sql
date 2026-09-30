WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rnk
    FROM inventory
)
SELECT *
FROM ranked_inventory
WHERE rnk <= 3
