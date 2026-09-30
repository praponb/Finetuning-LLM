WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.brand,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.tags,
        latest.last_restock,
        latest.inventory_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN cost >= 670 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    brand,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    COUNT_IF(category = 'Sports') AS sports_events,
    COUNT_IF(category = 'Grocery') AS grocery_events,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    GROUPING_ID() AS grouping_level
FROM with_last
GROUP BY GROUPING SETS ((CASE WHEN cost >= 670 THEN 'LARGE' ELSE 'SMALL' END, brand), (CASE WHEN cost >= 670 THEN 'LARGE' ELSE 'SMALL' END))
HAVING sports_events > 1
