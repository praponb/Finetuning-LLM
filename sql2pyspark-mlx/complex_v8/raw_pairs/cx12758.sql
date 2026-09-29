WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND channel = 'phone'
),
deduped AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id DESC) AS recency_rank
        FROM filtered_tickets
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND MONTH(t.event_at) IN (1, 3)
              AND t.event_type <> 'replied'
        )
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        agent_id,
        SUM(requester_id) AS total_requester_id,
        MAX(opened_at) AS last_opened_at,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id
    FROM without_matches
    GROUP BY TO_DATE(opened_at), agent_id
    HAVING COUNT(*) >= 2
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    distinct_agent_ids,
    GROUPING_ID() AS grouping_level,
    MAX(opened_at_day) AS last_opened_at_day,
    COUNT(DISTINCT agent_id) AS distinct_agent_ids_2,
    DATEDIFF(MAX(opened_at_day), MIN(opened_at_day)) AS opened_at_day_span_days
FROM rollup_base
GROUP BY GROUPING SETS ((agent_id, distinct_agent_ids), (agent_id), ())
