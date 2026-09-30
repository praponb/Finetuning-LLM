WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN rating >= 4.4 THEN 'High' WHEN rating >= 1.6 THEN 'Medium' ELSE 'Low' END rating_band,
        COALESCE(city, 'DIRECT') city_clean
    FROM suppliers
    WHERE city = 'Tokyo'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.launch_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(rating, 2) rating_rounded,
        DATE_ADD(launch_date, 19) launch_date_plus_19d,
        COALESCE(city, 'UNKNOWN') city_clean_2
    FROM with_latest
    WHERE city IN ('Chiang Mai', 'Singapore')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM base
    INTERSECT
    SELECT country
    FROM suppliers
    WHERE country = 'UK'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.country = k.country
)
SELECT
    supplier_name,
    city,
    rating,
    rating_band,
    city_clean,
    launch_date,
    launch_date_plus_19d,
    city_clean_2
FROM reconciled
ORDER BY launch_date_plus_19d
