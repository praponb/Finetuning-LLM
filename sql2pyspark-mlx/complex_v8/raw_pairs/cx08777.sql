WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE premium >= 2111.3
      AND status IS NOT NULL
)
SELECT
    holder_id,
    status AS name,
    start_date,
    'policy' AS origin
FROM prepared
WHERE status = 'LAPSED'
UNION ALL
SELECT
    id AS holder_id,
    status AS name,
    start_date,
    'policy_2' AS origin
FROM policies
WHERE status IN ('CANCELLED', 'LAPSED')
  AND premium <= 3719.7
ORDER BY holder_id
