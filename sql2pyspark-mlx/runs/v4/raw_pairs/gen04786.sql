WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(price) OVER w_recent price_avg_2,
        SUM(stock_qty) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_stock_qty
    FROM products
    WINDOW w_recent AS (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics_products AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY running_stock_qty DESC, id) row_seq,
        LAST_VALUE(price_avg_2) IGNORE NULLS OVER w_part last_known_price_avg_2
    FROM metrics
    WINDOW w_part AS (PARTITION BY category, brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        d.supplier_name,
        d.rating,
        d.city
    FROM metrics_products src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    (SELECT MIN(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_cost
FROM with_dims s
ORDER BY min_product_cost DESC NULLS LAST;
