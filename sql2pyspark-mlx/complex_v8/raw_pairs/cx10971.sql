WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LEAST(requester_id, csat) AS least_requester_id_csat
    FROM tickets
    WHERE requester_id >= 8
      AND channel IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MAX(opened_at) AS last_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_opened_at
    FROM base s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(last_ticket_opened_at), agent_id ORDER BY last_ticket_opened_at DESC NULLS FIRST, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(last_ticket_opened_at), agent_id ORDER BY last_ticket_opened_at DESC NULLS FIRST), 3) AS last_ticket_opened_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(last_ticket_opened_at), agent_id ORDER BY last_ticket_opened_at DESC NULLS FIRST) AS last_ticket_opened_at_dense_rank
    FROM with_counts
)
SELECT *
FROM (
    SELECT csat, closed_at, channel, least_requester_id_csat
    FROM with_trends
) src
PIVOT (
    MAX(least_requester_id_csat)
    FOR channel IN ('email', 'chat', 'phone')
)
ORDER BY csat, closed_at
