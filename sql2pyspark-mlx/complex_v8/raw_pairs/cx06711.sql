WITH combined AS (
    -- Stack the branches
    SELECT
        'policy' AS src,
        holder_id AS entity_id,
        end_date AS event_date,
        id
    FROM policies
    UNION ALL
    SELECT
        'policy_2' AS src,
        holder_id,
        end_date,
        id
    FROM policies
    WHERE status = 'ACTIVE'
      AND start_date BETWEEN DATE '2024-03-15' AND DATE '2025-05-28'
)
SELECT src, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY src
ORDER BY src
