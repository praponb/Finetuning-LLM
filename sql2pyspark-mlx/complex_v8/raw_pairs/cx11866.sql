WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium < 3465.4
),
all_events AS (
    -- Stack the branches
    SELECT holder_id, premium, 'policy' AS origin
    FROM recent
    UNION ALL
    SELECT id AS holder_id, risk_score AS premium, 'holder' AS origin
    FROM policyholders
    WHERE joined_on BETWEEN DATE '2024-02-04' AND DATE '2025-05-01'
)
SELECT origin, COUNT(*) AS n_rows, SUM(holder_id) AS total_holder_id
FROM all_events
GROUP BY origin
ORDER BY origin
