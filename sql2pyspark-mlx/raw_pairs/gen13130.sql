WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(attr_key) AS attr_key_upper
    FROM flattened
    WHERE attr_key IS NOT NULL
      AND attr_key = 'version'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE service = 'billing'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(attr_key) AS attr_key_length
    FROM verified
    WHERE attr_key IS NOT NULL
      AND attr_key <> 'region'
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE level IN ('WARN', 'INFO', 'DEBUG')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys_2 k ON s.id = k.id
)
-- Grouped metrics
SELECT
    attr_key,
    SUM(CASE WHEN attr_key = 'trace' THEN attr_key_length ELSE 0 END) AS trace_attr_key_length,
    COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) AS trace_count
FROM reconciled
GROUP BY attr_key
HAVING COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) > 1
ORDER BY trace_count DESC
