WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(attr_key, ' ', host) AS attr_key_host
    FROM lines
    WHERE attr_key IS DISTINCT FROM 'trace'
),
kept_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM valid
    EXCEPT
    SELECT host, id
    FROM app_logs
    WHERE host = 'h4'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.host = k.host AND s.id = k.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE level IN ('ERROR', 'WARN')
),
kept_rows_app_logs AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys_2 k ON s.id = k.id
)
SELECT value
FROM kept_rows_app_logs;
