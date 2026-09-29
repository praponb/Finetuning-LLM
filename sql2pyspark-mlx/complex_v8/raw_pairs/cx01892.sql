WITH unioned AS (
    -- Stack the branches
    SELECT holder_id AS qty, premium, 'policy' AS origin
    FROM policies
    WHERE status IN ('CANCELLED', 'LAPSED', 'EXPIRED')
      AND holder_id IS NOT NULL
    UNION
    SELECT id, risk_score, 'holder' AS origin
    FROM policyholders
    WHERE holder_name IS NOT NULL
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY origin
ORDER BY origin
