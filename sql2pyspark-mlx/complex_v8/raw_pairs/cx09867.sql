WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE csat < 4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel, agent_id ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY closed_at ASC, id) AS closed_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY days_since_closed_at, id) AS days_since_closed_at_seq,
        NTILE(5) OVER (PARTITION BY channel, agent_id ORDER BY closed_at ASC, id) AS closed_at_quintile,
        SUM(days_since_closed_at) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_sum_days_since_closed_at
    FROM valid
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.second_closed_at,
        s.closed_at_dense_rank,
        s.channel_agent_id_sum_days_since_closed_at,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM with_trends s
    JOIN ticket_events t ON t.ticket_id = s.id AND TO_DATE(t.event_at) < DATE '2025-02-06'
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    second_closed_at,
    closed_at_dense_rank,
    event_id,
    LEAST(requester_id, closed_at_dense_rank) AS least_requester_id_closed_at_dense_rank,
    DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
FROM line_level_events
ORDER BY id
