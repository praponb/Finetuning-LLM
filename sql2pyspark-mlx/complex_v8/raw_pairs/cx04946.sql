WITH valid_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE region IN ('East', 'West', 'North')
)
SELECT id AS qty
FROM valid_holders
WHERE LENGTH(TRIM(holder_name)) >= 5
EXCEPT
SELECT level AS qty
FROM adjusters
ORDER BY qty DESC
