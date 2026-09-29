WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        rating,
        UPPER(country) AS country_upper
    FROM suppliers
    WHERE country = 'TH'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.price
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS FIRST, id) AS rn
    FROM with_latest
    QUALIFY rn = 1
),
with_latest_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.launch_date,
        x.brand
    FROM latest s
    JOIN LATERAL (
            SELECT c.launch_date, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT country, id
    FROM with_latest_suppliers
    INTERSECT
    SELECT country, id
    FROM suppliers
    WHERE city = 'London'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest_suppliers s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.id = k.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        brand,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT brand) AS distinct_brands,
        SIZE(COLLECT_SET(brand)) AS num_brands
    FROM reconciled
    GROUP BY brand
)
SELECT
    brand,
    avg_price,
    distinct_brands,
    num_brands
FROM totals;
