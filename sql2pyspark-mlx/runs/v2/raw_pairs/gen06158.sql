WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
    WHERE last_restock BETWEEN '2024-06-03' AND '2024-08-31'
)
SELECT id, product_id, last_restock
FROM ranked_inventory
WHERE rnk <= 3
ORDER BY product_id DESC, id
