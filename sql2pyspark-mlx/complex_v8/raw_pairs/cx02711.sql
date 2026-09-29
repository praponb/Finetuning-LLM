WITH filtered AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        LENGTH(TRIM(segment)) AS segment_len
    FROM policyholders
    WHERE (joined_on >= DATE '2024-07-18' OR LOWER(holder_name) LIKE '%su%')
)
SELECT segment
FROM filtered
EXCEPT ALL
SELECT region
FROM policyholders
WHERE risk_score >= 0.3
