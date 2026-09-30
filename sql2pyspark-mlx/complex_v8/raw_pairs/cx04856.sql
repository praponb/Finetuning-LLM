WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        ABS(csat - 2) AS csat_distance,
        CASE WHEN csat >= 3 THEN 'large' WHEN csat >= 2 THEN 'medium' ELSE 'small' END AS csat_tier,
        ABS(csat - 4) AS csat_distance_2
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-14 22:00:00'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        HOUR(opened_at) AS opened_at_hour,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        COALESCE(csat_distance, 0) AS csat_distance_filled
    FROM filtered
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, opened_at_hour, csat, csat_distance, csat_distance_filled
    FROM scored
) src
UNPIVOT (
    amount FOR field IN (opened_at_hour, csat, csat_distance, csat_distance_filled)
) unp
