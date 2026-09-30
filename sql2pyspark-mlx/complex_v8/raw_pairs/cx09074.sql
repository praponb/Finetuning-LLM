WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE channel = 'chat'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        MAX(csat) OVER (PARTITION BY csat) AS csat_max_csat,
        SUM(requester_id) OVER w_csat AS csat_sum_requester_id
    FROM valid_tickets
    WINDOW w_csat AS (PARTITION BY csat)
),
agent_id_stats AS (
    -- Median csat_max_csat per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat_max_csat, 0.5) AS median_csat_max_csat
    FROM ranked
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_csat_max_csat,
        ROUND(s.csat_max_csat - g.median_csat_max_csat, 2) AS csat_max_csat_minus_median
    FROM ranked s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat_max_csat >= g.median_csat_max_csat
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    tags,
    second_csat,
    csat_max_csat,
    csat_sum_requester_id,
    median_csat_max_csat,
    csat_max_csat_minus_median,
    ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
FROM vs_group
ORDER BY csat_max_csat_minus_median DESC
