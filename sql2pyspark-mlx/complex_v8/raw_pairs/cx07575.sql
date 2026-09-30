WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE (requester_id < 15 OR csat <= 4)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        csat,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
        MIN(closed_at) AS first_closed_at,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM valid_tickets
    WHERE agent_id <= 8
    GROUP BY 1
    HAVING MAX(csat) > 3
),
labeled AS (
    -- Label and score rows
    SELECT *, ABS(csat - 3) AS csat_distance
    FROM rollup_base
)
SELECT
    csat,
    first_closed_at,
    csat_distance,
    COALESCE(csat_distance, 0) AS csat_distance_filled,
    DATE_FORMAT(first_closed_at, 'MMM yyyy') AS first_closed_at_label
FROM labeled
WHERE csat < 5
ORDER BY csat DESC, first_closed_at DESC NULLS FIRST
