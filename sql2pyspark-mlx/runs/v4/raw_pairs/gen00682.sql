WITH exploded AS (
    SELECT
        id,
        created_at,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
)
SELECT
    id,
    key,
    CASE WHEN key RLIKE '^(utm_source|gift)$' THEN 'EXTENDED' WHEN key RLIKE '^(device|weight_kg)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS key_zone
FROM exploded
ORDER BY key DESC NULLS LAST;
