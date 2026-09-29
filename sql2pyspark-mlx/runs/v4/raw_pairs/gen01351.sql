WITH scored AS (
    SELECT
        *,
        LAST_VALUE(revenue, TRUE) OVER (ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_revenue,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) revenue_rank
    FROM store_sales
)
SELECT
    s.id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.revenue_rank,
    (SELECT MIN(o2.quantity) FROM order_items o2 WHERE o2.product_id = s.product_id) AS min_order_item_quantity
FROM scored s
ORDER BY sale_date ASC NULLS FIRST
