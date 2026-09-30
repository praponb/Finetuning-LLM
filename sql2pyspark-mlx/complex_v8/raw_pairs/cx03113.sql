WITH cleaned_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_label
    FROM policyholders
    WHERE risk_score BETWEEN 0.2 AND 0.6
      AND holder_name IS NOT NULL
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.id) AS avg_policy_premium
    FROM cleaned_holders s
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.product <> 'LIFE'
              AND p.status IS NOT NULL
        )
)
SELECT *
FROM (
    SELECT joined_on, segment, avg_policy_premium
    FROM qualified
) src
PIVOT (
    MIN(avg_policy_premium)
    FOR segment IN ('RETAIL', 'VIP')
)
ORDER BY joined_on
