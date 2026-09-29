WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(element) AS element_lower,
        COALESCE(element, 'N/A') AS element_filled
    FROM flattened
    WHERE created_at >= DATE '2025-03-02'
      AND customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 9 AND 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, b.name
    FROM kept_rows src
    JOIN customers b
        ON src.customer_id = b.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(name) AS name_length,
        UPPER(element) AS element_upper,
        COALESCE(element, 'N/A') AS element_filled_2
    FROM with_dims
    WHERE created_at > DATE '2025-03-02'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.name AS customer_name,
        b.country
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        created_at,
        element,
        element_lower,
        element_filled,
        name,
        name_length,
        element_upper,
        element_filled_2,
        country,
        LOWER(element) AS element_lower_2,
        CAST(name_length AS DOUBLE) AS name_length_num,
        COALESCE(element, 'DIRECT') AS element_clean
    FROM enriched
    WHERE created_at > TIMESTAMP '2025-03-02 12:00:00'
)
-- Grouped metrics
SELECT
    customer_id,
    CASE WHEN name_length >= 66 THEN 'LARGE' ELSE 'SMALL' END AS name_length_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(CASE WHEN element = 'SPRING10' THEN 1 END) AS spring10_count,
    GROUPING(customer_id) AS grouping_level
FROM valid_carts
GROUP BY ROLLUP (customer_id, CASE WHEN name_length >= 66 THEN 'LARGE' ELSE 'SMALL' END)
HAVING distinct_countries > 1;
