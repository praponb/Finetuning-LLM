WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(unit_price) OVER w_recent AS running_max_unit_price,
        MIN(unit_price) OVER w_recent AS running_min_unit_price,
        SUM(unit_price) OVER w_recent AS running_sum_unit_price,
        AVG(unit_price) OVER w_recent AS running_avg_unit_price
    FROM valid
    WHERE unit_price > 310
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.stock_qty
    FROM scored s
    LEFT JOIN products p
        ON s.product_id <=> p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 4) AS quantity_share,
        SUM(stock_qty) OVER (ORDER BY running_avg_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        DENSE_RANK() OVER (ORDER BY running_avg_unit_price DESC) AS running_avg_unit_price_dense_rank,
        SUM(running_sum_unit_price) OVER (PARTITION BY product_id ORDER BY running_max_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_unit_price
    FROM with_dims
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, o.shipping_fee
    FROM enriched s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(shipping_fee, 0) AS shipping_fee_filled
    FROM joined
    WHERE running_sum_unit_price < 1000
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.running_min_unit_price,
        a.running_avg_unit_price,
        a.quantity_share,
        a.cumulative_stock_qty,
        a.running_avg_unit_price_dense_rank,
        a.shipping_fee,
        d.product_name,
        d.cost
    FROM filtered a
    LEFT JOIN products d
        ON a.product_id = d.id
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS num_rows,
    MIN(s.quantity_share) AS min_quantity_share,
    MIN(s.running_avg_unit_price_dense_rank) AS min_running_avg_unit_price_dense_rank
FROM enriched_order_items s
INNER JOIN products p ON s.product_id = p.id
GROUP BY p.category
ORDER BY min_quantity_share NULLS FIRST
