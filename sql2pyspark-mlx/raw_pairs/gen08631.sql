WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC) AS latest_rank
    FROM store_sales
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.latest_rank,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM ranked s
ORDER BY revenue DESC NULLS LAST, min_inventory_quantity;
