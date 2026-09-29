WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix,
        CASE WHEN country IN ('US', 'UK') THEN 'Group A' ELSE 'Other' END AS country_group,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.cost,
        top1.product_brand
    FROM filtered s
    JOIN LATERAL (
            SELECT c.cost, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM latest_match
    INTERSECT
    SELECT id
    FROM suppliers
    WHERE rating IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
FROM reconciled
GROUP BY city
