WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.code,
        d.name
    FROM exploded e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.segment,
        d.city
    FROM with_dims e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
    WHERE LENGTH(d.name) < 6
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        name,
        segment,
        UPPER(TRIM(name)) AS name_normalized
    FROM joined
    WHERE city IN ('Singapore', 'New York', 'Berlin')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', segment, name) AS segment_name_key,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM valid
    WHERE segment IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(name) AS name_upper,
        CONCAT_WS('-', name, segment) AS name_segment_key,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM filtered
    WHERE segment IS NOT DISTINCT FROM 'Corporate'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        name,
        segment,
        name_normalized,
        segment_name_key,
        name_upper,
        name_segment_key,
        CONCAT_WS('-', name, segment) AS name_segment_key_2,
        INITCAP(TRIM(segment)) AS segment_title
    FROM base
    WHERE segment IS NOT DISTINCT FROM 'Consumer'
)
SELECT
    name,
    segment,
    name_normalized,
    segment_name_key,
    name_upper,
    name_segment_key_2,
    segment_title,
    CASE WHEN name RLIKE '^(Pim Chaiyaporn|Jack Taylor)$' THEN 'GROUP_B' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS name_zone
FROM cleaned
ORDER BY segment DESC NULLS FIRST;
