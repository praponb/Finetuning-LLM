WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN cost >= 650 THEN 'High' WHEN cost >= 280 THEN 'Medium' ELSE 'Low' END AS cost_level
    FROM products
    WHERE launch_date >= DATE '2023-08-22'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE launch_date < '2024-03-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        product_name,
        category,
        stock_qty,
        launch_date,
        supplier_id,
        ROUND(price, 1) AS price_rounded
    FROM reconciled
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Stark'
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.city,
        d.country,
        d.rating
    FROM valid e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.country AS supplier_country,
        s.rating AS supplier_rating
    FROM enriched e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            supplier_country,
            category,
            stock_qty
        FROM with_dims
    ) src
    PIVOT (
        MAX(stock_qty)
        FOR category IN ('Electronics', 'Sports', 'Home')
    )
)
SELECT
    supplier_country,
    Sports,
    Home
FROM pivoted
