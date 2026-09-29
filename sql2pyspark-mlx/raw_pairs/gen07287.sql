WITH with_last AS (
    SELECT
        s.*,
        x.inventory_quantity,
        x.inventory_last_restock
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    supplier_id,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(*) FILTER (WHERE category = 'Fashion') AS fashion_rows,
    ROUND(AVG(inventory_quantity), 2) AS avg_inventory_quantity,
    STDDEV_SAMP(cost) AS cost_stddev,
    COUNT_IF(category = 'Sports') AS sports_events
FROM with_last
GROUP BY 1
HAVING COUNT(*) FILTER (WHERE category = 'Fashion') > 1
