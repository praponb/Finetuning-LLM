WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-28 19:00:00'
      AND ARRAY_CONTAINS(tags, 'outage')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(closed_at) OVER w_all_by_opened_at AS next_closed_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY opened_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(opened_at_plus_30m), agent_id ORDER BY opened_at DESC, id), 3) AS opened_at_percent_rank,
        DENSE_RANK() OVER w_opened_at_plus_30m_month_age AS opened_at_dense_rank
    FROM filtered
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at, id), w_opened_at_plus_30m_month_age AS (PARTITION BY MONTH(opened_at_plus_30m), agent_id ORDER BY opened_at DESC, id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2
    FROM metrics s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(minutes_since_prev_closed_at) IGNORE NULLS OVER (PARTITION BY channel, agent_id ORDER BY first_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_minutes_since_prev_closed_at,
        DENSE_RANK() OVER w_channel_agent_id_by_first_ev_2 AS first_event_at_dense_rank,
        ROUND(AVG(minutes_since_prev_closed_at) OVER w_opened_at_plus_30m_year, 2) AS opened_at_plus_30m_year_avg_minutes_since_prev_c,
        NTILE(3) OVER w_channel_agent_id_by_first_ev_3 AS first_event_at_tercile
    FROM summarized_rows
    WINDOW w_channel_agent_id_by_first_ev_2 AS (PARTITION BY channel, agent_id ORDER BY first_event_at), w_opened_at_plus_30m_year AS (PARTITION BY YEAR(opened_at_plus_30m)), w_channel_agent_id_by_first_ev_3 AS (PARTITION BY channel, agent_id ORDER BY first_event_at, id)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.opened_at_plus_30m,
        s.channel_priority_label,
        s.minutes_since_prev_closed_at,
        s.opened_at_dense_rank,
        s.first_event_at,
        s.first_event_at_2,
        s.last_known_minutes_since_prev_closed_at,
        s.opened_at_plus_30m_year_avg_minutes_since_prev_c,
        s.first_event_at_tercile,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM sequenced s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IS NOT NULL
)
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    channel_priority_label,
    minutes_since_prev_closed_at,
    opened_at_dense_rank,
    first_event_at,
    last_known_minutes_since_prev_closed_at,
    first_event_at_tercile,
    event_id,
    event_at,
    event_type,
    FLOOR(first_event_at_tercile / 2) * 2 AS first_event_at_tercile_bucket,
    DATE_TRUNC('QUARTER', opened_at_plus_30m) AS opened_at_plus_30m_quarter
FROM detail_rows_events
ORDER BY channel DESC
