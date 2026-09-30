WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat - requester_id AS csat_minus_requester_id,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE channel NOT IN ('chat', 'web')
      AND csat > 2
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat_minus_requester_id,
        s.requester_id_plus_csat,
        su.team AS agent_team
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        closed_at,
        channel,
        csat_minus_requester_id,
        requester_id_plus_csat,
        agent_team,
        closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d,
        LEAST(csat_minus_requester_id, requester_id_plus_csat) AS least_csat_minus_requester_id_requester_id_plus_,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label
    FROM enriched
),
channel_stats AS (
    -- Avg csat_minus_requester_id per channel
    SELECT channel, AVG(csat_minus_requester_id) AS avg_csat_minus_requester_id
    FROM enriched_2
    GROUP BY channel
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_csat_minus_requester_id,
        ROUND(s.csat_minus_requester_id - g.avg_csat_minus_requester_id, 2) AS csat_minus_requester_id_minus_avg
    FROM enriched_2 s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat_minus_requester_id > g.avg_csat_minus_requester_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(requester_id_plus_csat) OVER (PARTITION BY channel, agent_team) AS channel_agent_team_min_requester_id_plus_csat,
        LEAD(csat_minus_requester_id) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY least_csat_minus_requester_id_requester_id_plus_ DESC NULLS FIRST, id) AS next_csat_minus_requester_id,
        NTILE(3) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY least_csat_minus_requester_id_requester_id_plus_ DESC NULLS FIRST, id) AS least_csat_minus_requester_id_requester_id_plus__2,
        NTH_VALUE(closed_at_plus_7d, 2) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY least_csat_minus_requester_id_requester_id_plus_ DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_plus_7d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY least_csat_minus_requester_id_requester_id_plus_ DESC NULLS FIRST), 3) AS least_csat_minus_requester_id_requester_id_plus__3,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY least_csat_minus_requester_id_requester_id_plus_ DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_closed_at
    FROM above_average
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE event_type <> 'assigned') AS event_event_type_count,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM with_windows s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    closed_at,
    channel,
    csat_minus_requester_id,
    requester_id_plus_csat,
    agent_team,
    closed_at_plus_7d,
    least_csat_minus_requester_id_requester_id_plus_,
    closed_at_label,
    csat_minus_requester_id_minus_avg,
    channel_agent_team_min_requester_id_plus_csat,
    next_csat_minus_requester_id,
    least_csat_minus_requester_id_requester_id_plus__2,
    minutes_since_prev_closed_at,
    event_event_type_count,
    event_count_2,
    LENGTH(TRIM(agent_team)) AS agent_team_len
FROM summarized_rows
WHERE minutes_since_prev_closed_at < 4500.0
