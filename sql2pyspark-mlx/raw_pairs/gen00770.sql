WITH lines AS (
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, value
)
SELECT
    attr_key,
    MAX(created_at) AS last_seen,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count
FROM lines
GROUP BY attr_key;
