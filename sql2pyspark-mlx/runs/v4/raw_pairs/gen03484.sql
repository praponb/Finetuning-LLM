WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(attr_key) AS clean_attr_key
    FROM lines
    WHERE attr_key = 'utm_source'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(attr_key, 1, 2) AS attr_key_prefix
    FROM base
    WHERE attr_key IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key_2,
        TRIM(attr_key) AS clean_attr_key_3
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        attr_key,
        attr_key_prefix,
        CASE WHEN attr_key IN ('weight_kg', 'utm_source') THEN 'Group A' ELSE 'Other' END AS attr_key_group
    FROM valid
    WHERE attr_key IS NOT NULL
      AND attr_key = 'weight_kg'
),
valid_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM valid_carts
    WHERE attr_key IS NOT NULL
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_key,
        attr_key_group,
        clean_attr_key,
        attr_key_filled,
        TRIM(attr_key) AS clean_attr_key_2,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM valid_carts_2
    WHERE attr_key IS NOT NULL
      AND attr_key != 'utm_source'
),
stats AS (
    -- Multi-level aggregation
    SELECT
        attr_key,
        COUNT(DISTINCT attr_key) AS distinct_attr_keys,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys_2,
        GROUPING(attr_key) AS grouping_id
    FROM base_carts
    GROUP BY ROLLUP (attr_key)
)
SELECT
    attr_key,
    distinct_attr_keys,
    attr_key_list,
    grouping_id
FROM stats
ORDER BY attr_key;
