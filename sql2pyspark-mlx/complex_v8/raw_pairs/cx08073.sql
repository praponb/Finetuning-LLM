WITH combined AS (
    -- Stack the branches
    SELECT
        'holder' AS record_type,
        id AS entity_id,
        risk_score,
        joined_on AS event_date
    FROM policyholders
    WHERE risk_score < 0.8
      AND region <> 'South'
    UNION ALL
    SELECT
        'policy' AS record_type,
        holder_id,
        premium,
        end_date
    FROM policies
    WHERE status IN ('CANCELLED', 'EXPIRED', 'LAPSED')
      AND end_date < DATE '2026-09-23'
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY record_type
ORDER BY record_type
