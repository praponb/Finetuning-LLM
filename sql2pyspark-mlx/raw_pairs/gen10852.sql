WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CONCAT(category, ', ', product_name) AS category_product_name
    FROM products
    WHERE brand IS DISTINCT FROM 'Globex'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.category,
        src.cost,
        src.stock_qty,
        src.tags,
        src.supplier_id,
        src.category_product_name,
        p.city
    FROM base src
    LEFT JOIN suppliers p
        ON src.supplier_id <=> p.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, stock_qty * cost AS stock_qty_times_cost
    FROM joined
    WHERE cost BETWEEN 380 AND 640
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        category,
        cost,
        supplier_id,
        category_product_name,
        city,
        stock_qty_times_cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY stock_qty_times_cost DESC, id ASC) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
)
-- Join a dimension and aggregate
SELECT
    dim.city AS supplier_city,
    e.category,
    COUNT(*) AS num_rows,
    AVG(e.stock_qty_times_cost) AS avg_stock_qty_times_cost,
    ROUND(AVG(e.cost), 2) AS avg_cost
FROM ranked e
INNER JOIN suppliers dim ON e.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.city, e.category), (dim.city))
ORDER BY num_rows ASC NULLS LAST
