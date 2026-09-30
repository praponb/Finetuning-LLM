WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.tags,
        src.supplier_id,
        p.rating,
        p.city
    FROM products src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
    WHERE p.supplier_name IS NOT NULL
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.supplier_name,
        d.rating AS supplier_rating,
        d.city AS supplier_city
    FROM joined e
    INNER JOIN suppliers d
        ON e.supplier_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(supplier_rating, TRUE) OVER (PARTITION BY supplier_city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_supplier_rating,
        SUM(supplier_rating) OVER (ORDER BY supplier_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_supplier_rating,
        LAST_VALUE(rating, TRUE) OVER (PARTITION BY city ORDER BY supplier_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating
    FROM enriched
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        brand,
        tags,
        supplier_id,
        rating,
        supplier_city,
        last_known_supplier_rating,
        running_supplier_rating,
        CONCAT(city, '/', supplier_name) AS city_supplier_name,
        TRIM(brand) AS clean_brand
    FROM with_windows
    WHERE brand IS DISTINCT FROM 'Umbrella'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY supplier_city ORDER BY rating DESC) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_supplier_rating >= 20 THEN 'LARGE' ELSE 'SMALL' END AS running_supplier_rating_tier,
    supplier_id,
    MAX(last_known_supplier_rating) FILTER (WHERE brand = 'Globex') AS globex_max_last_known_supplier_rating,
    ROUND(SUM(running_supplier_rating) / NULLIF(COUNT(CASE WHEN supplier_city = 'Singapore' THEN 1 END), 0), 2) AS running_supplier_rating_per_singapore,
    COUNT(CASE WHEN supplier_city = 'Bangkok' THEN 1 END) AS bangkok_count,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    SUM(last_known_supplier_rating) FILTER (WHERE brand = 'Initech') AS initech_total_last_known_supplier_rating,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN running_supplier_rating >= 20 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(last_known_supplier_rating) FILTER (WHERE brand = 'Initech') DESC) AS initech_total_last_known_supplier_rating_percent_rank
FROM latest
GROUP BY CASE WHEN running_supplier_rating >= 20 THEN 'LARGE' ELSE 'SMALL' END, supplier_id
HAVING bangkok_count > 1
ORDER BY bangkok_count DESC;
