WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policyholders
    WHERE joined_on >= DATE '2024-07-18'
      AND LOWER(holder_name) LIKE '%o %'
)
SELECT segment
FROM filtered
INTERSECT
SELECT adjuster_name AS segment
FROM adjusters
WHERE LOWER(adjuster_name) LIKE '%on%'
  AND level < 3
ORDER BY segment DESC
