WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-08-20')
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.price,
        p.category
    FROM filtered src
    INNER JOIN products p
        ON src.product_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(units, 2) AS units_rounded,
        price + revenue_per_unit AS price_plus_revenue_per_unit
    FROM enriched
    WHERE sale_date <= '2024-10-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM valid
    EXCEPT ALL
    SELECT id, product_id
    FROM store_sales
    WHERE product_id IN (1, 2, 10)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
)
-- Join a dimension and aggregate
SELECT
    d.city,
    s.category,
    COUNT(*) AS cnt,
    SUM(s.price) AS total_price,
    SUM(s.revenue) AS total_revenue
FROM reconciled s
INNER JOIN stores d ON s.store_id = d.id
GROUP BY d.city, s.category
ORDER BY city DESC NULLS LAST;
