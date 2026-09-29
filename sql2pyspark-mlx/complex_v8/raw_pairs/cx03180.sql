WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
grouped AS (
    -- Group-level totals
    SELECT
        agent_id,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
        COUNT(DISTINCT channel) AS distinct_channels,
        SUM(requester_id) AS total_requester_id,
        SUM(CASE WHEN priority = 'P2' THEN 1 ELSE 0 END) AS n_priority_p2
    FROM recent_tickets
    GROUP BY agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(requester_id_to_csat_ratio - AVG(requester_id_to_csat_ratio) OVER w_agent_id, 2) AS requester_id_to_csat_ratio_vs_agent_id_avg,
        ROUND(AVG(n_priority_p2) OVER (PARTITION BY agent_id), 2) AS agent_id_avg_n_priority_p2,
        NTH_VALUE(total_requester_id, 2) OVER (PARTITION BY agent_id ORDER BY requester_id_to_csat_ratio, agent_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_requester_id,
        LAG(requester_id_to_csat_ratio) OVER (PARTITION BY agent_id ORDER BY requester_id_to_csat_ratio, agent_id) AS prev_requester_id_to_csat_ratio,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id
    FROM grouped
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority IS NOT NULL
              AND t.closed_at >= TIMESTAMP '2025-02-08 13:00:00'
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        SUM(requester_id) AS total_ticket_requester_id,
        SUM(requester_id) AS total_ticket_requester_id_2
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_requester_id,
        t.total_ticket_requester_id_2
    FROM with_activity s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY agent_id ORDER BY second_total_requester_id, agent_id) AS second_total_requester_id_quintile,
        ROUND(n_priority_p2 - AVG(n_priority_p2) OVER (PARTITION BY agent_id, n_priority_p2), 2) AS n_priority_p2_vs_agent_id_n_priority_p2_avg,
        ROUND(AVG(total_ticket_requester_id) OVER (PARTITION BY agent_id ORDER BY second_total_requester_id, agent_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS total_ticket_requester_id_avg_1_prior,
        SUM(total_ticket_requester_id_2) OVER (PARTITION BY agent_id ORDER BY second_total_requester_id, agent_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_ticket_requester_id_2_sum_7
    FROM profiled
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-02-26'
              AND su.team NOT IN ('Tech', 'Onboarding')
        )
)
SELECT
    agent_id,
    requester_id_to_csat_ratio,
    n_priority_p2,
    requester_id_to_csat_ratio_vs_agent_id_avg,
    agent_id_avg_n_priority_p2,
    second_total_requester_id,
    rows_in_agent_id,
    second_total_requester_id_quintile,
    n_priority_p2_vs_agent_id_n_priority_p2_avg,
    total_ticket_requester_id_2_sum_7,
    total_requester_id + distinct_channels AS total_requester_id_plus_distinct_channels
FROM qualified
ORDER BY total_requester_id_plus_distinct_channels DESC NULLS FIRST, n_priority_p2_vs_agent_id_n_priority_p2_avg, agent_id
LIMIT 20
