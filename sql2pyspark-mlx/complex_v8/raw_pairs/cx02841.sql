WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week,
        YEAR(opened_at) AS opened_at_year,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.requester_id), 2) FROM tickets t WHERE t.agent_id = s.agent_id) AS avg_ticket_requester_id
    FROM valid s
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_all_by_avg_ticket_requester AS avg_ticket_requester_id_quintile,
        RANK() OVER (ORDER BY avg_ticket_requester_id, id) AS avg_ticket_requester_id_rank
    FROM with_stats
    WINDOW w_all_by_avg_ticket_requester AS (ORDER BY avg_ticket_requester_id, id)
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.opened_at_year,
        s.avg_ticket_requester_id_rank,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_details AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.opened_at_year,
        s.avg_ticket_requester_id_rank,
        s.event_id,
        s.event_actor,
        t.actor AS event_actor_2
    FROM line_level s
    LEFT JOIN ticket_events t ON t.id = s.event_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        channel,
        opened_at_year,
        avg_ticket_requester_id_rank,
        event_id,
        event_actor,
        event_actor_2,
        COALESCE(opened_at_year, 0) AS opened_at_year_filled,
        COALESCE(opened_at_year, 0) AS opened_at_year_filled_2,
        COALESCE(event_actor_2, 'UNKNOWN') AS event_actor_2_or_unknown
    FROM with_details
),
totals AS (
    -- Summarise per group
    SELECT
        event_actor_2,
        avg_ticket_requester_id_rank,
        event_id,
        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS n_channel_web,
        COUNT(*) AS row_count,
        SUM(CASE WHEN channel NOT IN ('web', 'email') THEN 1 ELSE 0 END) AS n_channel_not_in_web_email
    FROM enriched
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
)
SELECT
    event_actor_2,
    avg_ticket_requester_id_rank,
    event_id,
    n_channel_web,
    row_count,
    n_channel_not_in_web_email,
    n_channel_web + row_count AS n_channel_web_plus_row_count
FROM totals
ORDER BY row_count, event_actor_2
