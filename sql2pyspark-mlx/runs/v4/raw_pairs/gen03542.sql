WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock >= DATE '2023-03-06'
      AND YEAR(last_restock) = 2024
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY days_since_last_restock, id) AS rank_in_group
    FROM filtered
) t
WHERE rank_in_group = 1
