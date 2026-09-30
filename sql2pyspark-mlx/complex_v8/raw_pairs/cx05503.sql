WITH valid_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        ROUND(risk_score * 1.5, 2) AS risk_score_adj
    FROM policyholders
    WHERE region IN ('South', 'East', 'North')
      AND risk_score > 0.2
),
unioned AS (
    -- Stack the branches
    SELECT
        'holder' AS record_type,
        segment,
        region AS label,
        id AS qty
    FROM policyholders
    UNION
    SELECT
        'adjuster' AS record_type,
        team,
        adjuster_name,
        id
    FROM adjusters
    WHERE level > 1
      AND team NOT IN ('Health', 'Travel')
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY record_type
ORDER BY record_type
