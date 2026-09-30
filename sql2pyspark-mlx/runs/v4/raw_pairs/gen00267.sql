WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ROUND(stock_qty / price, 2) AS stock_qty_per_price
    FROM products
    WHERE launch_date >= DATE '2023-06-14'
      AND supplier_id IS NOT NULL
      AND category NOT IN ('Books', 'Grocery')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, category
    FROM filtered
    EXCEPT
    SELECT id, category
    FROM products
    WHERE category IN ('Home', 'Sports')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.category = k.category
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.rating,
        p.country,
        p.supplier_name
    FROM kept_rows src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost >= 620 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    COUNT(CASE WHEN brand = 'Initech' THEN 1 END) AS initech_count,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev
FROM joined
GROUP BY 1;
