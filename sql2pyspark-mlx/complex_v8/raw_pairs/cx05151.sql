WITH prepared_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE region IS NOT NULL
      AND risk_score <= 0.6
)
SELECT id
FROM prepared_holders
WHERE risk_score > 0.1
  AND region = 'East'
INTERSECT
SELECT level AS id
FROM adjusters
WHERE team NOT IN ('Property', 'Travel')
  AND level < 3
ORDER BY id
