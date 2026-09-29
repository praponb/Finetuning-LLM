WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE requester_id < 16
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.hired_on BETWEEN DATE '2024-04-14' AND DATE '2025-02-27'
              AND su.team IN ('VIP', 'Onboarding')
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_priority_opened_at_to_date_b, 3) AS opened_at_cume_dist,
        LAST_VALUE(csat) OVER (PARTITION BY priority, TO_DATE(opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        LEAD(csat, 3) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS next3_csat,
        MAX(csat) OVER (PARTITION BY priority, agent_id) AS priority_agent_id_max_csat,
        MAX(csat) OVER (PARTITION BY priority, TO_DATE(opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_max_6_prior
    FROM qualified
    WINDOW w_priority_opened_at_to_date_b AS (PARTITION BY priority, TO_DATE(opened_at) ORDER BY opened_at DESC, id)
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count_2
    FROM with_trends s
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    opened_at_cume_dist,
    last_csat,
    next3_csat,
    priority_agent_id_max_csat,
    csat_max_6_prior,
    event_count_2,
    CONCAT(channel, '#', CAST(event_count AS STRING)) AS channel_ref
FROM with_counts
