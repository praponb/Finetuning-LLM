WITH lines AS (
    SELECT
        id,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
)
SELECT
    id,
    attr_key,
    v,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(version)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS attr_key_zone
FROM lines
