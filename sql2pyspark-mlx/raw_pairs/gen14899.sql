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
    created_at,
    key,
    ROW_NUMBER() OVER (PARTITION BY TO_DATE(created_at), key ORDER BY created_at DESC, id, key) AS event_seq
FROM exploded
WHERE key IN ('gift', 'device');
