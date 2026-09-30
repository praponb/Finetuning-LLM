WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
),
base AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_value,
        COALESCE(attr_key, 'unassigned') AS attr_key_filled,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM exploded
    WHERE created_at > TO_DATE('2025-03-01')
),
with_windows AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY created_at DESC, id, attr_key) AS event_seq
    FROM base
    WHERE attr_key IN ('device', 'weight_kg')
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY event_seq NULLS FIRST, id, attr_key) = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
islands AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_key_clean,
        event_seq,
        SUM(is_new_burst) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    created_at,
    attr_key,
    attr_key_clean,
    event_seq,
    session_no,
    CASE WHEN attr_key RLIKE '^(device|gift)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS attr_key_zone
FROM islands;
