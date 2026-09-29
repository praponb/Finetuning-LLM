WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE priority IN ('P1', 'P2', 'P3')
      AND closed_at >= TIMESTAMP '2025-02-02 05:00:00'
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IS NOT NULL
              AND su.hired_on IS NOT NULL
        )
),
deduped AS (
    -- Keep the latest row per channel
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY requester_id DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at
    FROM deduped s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.event_count,
        s.last_event_at,
        su.agent_name
    FROM summarized_rows s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'Billing'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        last_event_at,
        agent_name,
        agent_team,
        ROUND(AVG(event_count) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS event_count_avg_2_prior,
        LEAD(event_count) OVER w_channel_by_closed_at AS next_event_count
    FROM with_parent
    WINDOW w_channel_by_closed_at AS (PARTITION BY channel ORDER BY closed_at, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, csat, next_event_count
    FROM metrics
) src
UNPIVOT (
    amount FOR metric IN (csat, next_event_count)
) unp
