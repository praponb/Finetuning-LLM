WITH prepared AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE premium <= 4417.0
)
SELECT holder_id
FROM prepared
WHERE YEAR(end_date) = 2026
  AND product IN ('AUTO', 'LIFE')
INTERSECT ALL
SELECT adjuster_id AS holder_id
FROM claims
WHERE policy_id IS NOT NULL
  AND adjuster_id <= 4
ORDER BY holder_id
