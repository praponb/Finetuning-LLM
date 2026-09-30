WITH exploded AS (
    SELECT
        id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
)
SELECT
    id,
    created_at,
    attr_key,
    CASE WHEN attr_key RLIKE '^(device|gift)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(utm_source|weight_kg)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS attr_key_zone
FROM exploded
ORDER BY attr_key_zone DESC;
