WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        cost,
        tags,
        supplier_id,
        SUM(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        MAX(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        COUNT(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        MIN(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        SUM(price) OVER (PARTITION BY supplier_id, category ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM products
    WHERE MONTH(launch_date) IN (3, 8, 10)
)
SELECT
    s.id,
    s.cost,
    s.tags,
    s.supplier_id,
    s.running_sum_cost,
    s.running_max_cost,
    s.running_count_cost,
    s.running_min_cost,
    (SELECT MIN(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_stock_qty
FROM enriched s
ORDER BY id ASC NULLS LAST, running_count_cost DESC
