WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        ABS(requester_id - 15) AS requester_id_distance,
        csat + requester_id AS csat_plus_requester_id,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND channel IN ('email', 'phone')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_tickets
    WHERE requester_id_distance > (SELECT AVG(requester_id_distance) FROM filtered_tickets)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_all_by_closed_at)) / 60, 1) AS minutes_since_prev_closed_at,
        NTILE(10) OVER (ORDER BY closed_at DESC, id) AS closed_at_decile,
        ROUND(requester_id_distance / NULLIF(SUM(requester_id_distance) OVER (PARTITION BY csat), 0), 4) AS requester_id_distance_share_of_csat,
        SUM(csat) OVER (PARTITION BY csat) AS csat_sum_csat
    FROM screened
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(priority, 'ALL') AS priority,
    closed_at_decile,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(closed_at_decile) / NULLIF(SUM(requester_id_distance), 0), 3) AS closed_at_decile_to_requester_id_distance_ratio,
    MAX(closed_at) AS last_closed_at
FROM with_trends
GROUP BY ROLLUP(channel, priority, closed_at_decile)
ORDER BY channel NULLS LAST, priority NULLS LAST, closed_at_decile NULLS LAST
