WITH latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.sale_date,
        x.revenue
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        brand,
        cost,
        stock_qty,
        supplier_id,
        revenue,
        RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS LAST) cost_rank,
        LEAD(price, 1) OVER (PARTITION BY MONTH(launch_date), category ORDER BY stock_qty, id) next_price,
        CUME_DIST() OVER (PARTITION BY category, brand ORDER BY revenue DESC NULLS FIRST) revenue_cume_dist,
        SUM(price) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_price
    FROM latest_match
),
latest_match_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.cost,
        s.stock_qty,
        s.revenue,
        s.cost_rank,
        s.next_price,
        x.inventory_quantity
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost, 2) OVER (PARTITION BY category ORDER BY stock_qty, id) prev_cost,
        NTILE(5) OVER (PARTITION BY category ORDER BY inventory_quantity DESC, id) inventory_quantity_bucket,
        LAG(next_price, 1) OVER (PARTITION BY category ORDER BY revenue, id) prev_next_price
    FROM latest_match_products
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id = 5
        )
)
-- Window metrics over several different partitions
SELECT
    id,
    category,
    cost,
    prev_cost,
    inventory_quantity_bucket,
    prev_next_price,
    LAG(inventory_quantity_bucket, 1) OVER (PARTITION BY category ORDER BY prev_next_price, id) prev_inventory_quantity_bucket,
    LEAD(prev_next_price, 1, 0) OVER (PARTITION BY category ORDER BY inventory_quantity, id) next_prev_next_price,
    AVG(stock_qty) OVER (PARTITION BY category ORDER BY prev_cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) stock_qty_avg_2_prior
FROM not_flagged
WHERE next_price IS NULL
