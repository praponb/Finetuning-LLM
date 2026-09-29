WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY status_code, id, attr_key) = 1
),
latest_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id, attr_key) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
latest_app_logs_2 AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id, attr_key) AS latest_rank
    FROM latest_app_logs
    QUALIFY latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        CASE WHEN level = 'INFO' THEN 1 ELSE 0 END AS is_info,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM latest_app_logs_2
    WHERE attr_key IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    attr_key,
    v,
    rank_in_group,
    attr_key_upper,
    FIRST_VALUE(attr_key) IGNORE NULLS OVER w_grp AS first_known_attr_key,
    ROW_NUMBER() OVER w_part AS row_seq,
    LAG(is_info, 1, 0) OVER w_part AS prev_is_info
FROM cleaned
WHERE service = 'search'
WINDOW w_grp AS (PARTITION BY host ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY level ORDER BY latest_rank, id, attr_key);
