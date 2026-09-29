WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        p.rating,
        p.city
    FROM products e
    LEFT JOIN suppliers p
        ON e.supplier_id = p.id
),
joined_products AS (
    -- Attach suppliers attributes
    SELECT e.*, b.city AS supplier_city
    FROM joined e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT supplier_id
    FROM joined_products
    INTERSECT
    SELECT supplier_id
    FROM products
    WHERE cost IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined_products s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(brand) AS clean_brand,
        price - cost AS margin,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM reconciled
    WHERE launch_date > DATE '2024-03-19'
      AND brand != 'Stark'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        rating,
        city,
        supplier_city,
        clean_brand,
        resolved_brand,
        GREATEST(margin, rating) AS greatest_margin_rating
    FROM cleaned
    WHERE launch_date > DATE '2023-08-10'
      AND supplier_id IS NOT NULL
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        p.supplier_name,
        p.rating AS supplier_rating,
        p.city AS supplier_city_2
    FROM valid a
    INNER JOIN suppliers p
        ON a.supplier_id = p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(price, cost) OVER (PARTITION BY supplier_id) AS corr_price_cost,
        PERCENT_RANK() OVER (PARTITION BY supplier_city_2 ORDER BY supplier_rating DESC NULLS FIRST) AS supplier_rating_percent_rank
    FROM with_dims
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        supplier_city,
        greatest_margin_rating
    FROM enriched
) src
PIVOT (
    AVG(greatest_margin_rating) AS total,
    COUNT(1) AS n
    FOR supplier_city IN ('New York' AS new_york, 'Bangkok' AS bangkok, 'Singapore' AS singapore)
);
