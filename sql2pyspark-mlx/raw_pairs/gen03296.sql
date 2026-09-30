WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost <= 670
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT id, supplier_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS latest_rank
        FROM kept
    ) x
    WHERE latest_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(supplier_name) AS supplier_name_upper,
        LENGTH(supplier_name) AS supplier_name_length
    FROM ranked
    WHERE supplier_name LIKE '%Trader%'
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'bestseller')
        )
)
-- Grouped metrics
SELECT
    CASE WHEN supplier_name_length >= 65 THEN 'HIGH' ELSE 'LOW' END AS supplier_name_length_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(supplier_name_length, ARRAY(0.25, 0.5, 0.75)) AS supplier_name_length_quartiles
FROM kept_suppliers
GROUP BY CASE WHEN supplier_name_length >= 65 THEN 'HIGH' ELSE 'LOW' END
ORDER BY supplier_name_length_tier NULLS LAST
