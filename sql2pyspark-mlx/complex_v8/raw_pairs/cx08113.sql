WITH combined AS (
    -- Stack the branches
    SELECT
        'holder' AS record_type,
        id,
        risk_score AS metric,
        holder_name
    FROM policyholders
    UNION ALL
    SELECT
        'policy' AS record_type,
        holder_id AS id,
        premium AS metric,
        status AS holder_name
    FROM policies
)
SELECT record_type, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM combined
GROUP BY record_type
ORDER BY record_type
