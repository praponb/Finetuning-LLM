WITH valid_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM policyholders
    WHERE YEAR(joined_on) = 2025
)
SELECT id AS key_id
FROM valid_holders
EXCEPT
SELECT level
FROM adjusters
