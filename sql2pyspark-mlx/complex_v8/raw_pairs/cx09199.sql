WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE (channel = 'email' OR closed_at >= TIMESTAMP '2025-02-02 03:00:00')
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_tickets
    WHERE days_since_opened_at > (SELECT AVG(days_since_opened_at) FROM cleaned_tickets)
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND MONTH(t.closed_at) IN (1, 3)
        )
      AND s.channel = 'phone'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        tags,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, csat)) AS distinct_channel_in_agent_id_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_opened_at ASC), 3) AS days_since_opened_at_percent_rank,
        SUM(csat) OVER (ORDER BY days_since_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM screened
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.distinct_channel_in_agent_id_csat,
        s.days_since_opened_at_percent_rank,
        s.running_csat,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM sequenced s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.distinct_channel_in_agent_id_csat,
        s.days_since_opened_at_percent_rank,
        s.running_csat,
        s.event_id,
        s.event_type,
        s.event_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM detail_rows s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.agent_name IS NOT NULL
)
SELECT
    id,
    days_since_opened_at_percent_rank,
    running_csat,
    event_id,
    event_type,
    event_at,
    agent_team,
    agent_hired_on
FROM with_details
WHERE event_type = 'escalated'
ORDER BY days_since_opened_at_percent_rank DESC, agent_hired_on NULLS LAST, id, event_id
LIMIT 10
