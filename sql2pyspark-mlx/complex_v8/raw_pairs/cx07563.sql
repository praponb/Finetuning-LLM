WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
latest AS (
    -- Keep the earliest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_clean,
        event_at_week,
        actor_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at_week, id) AS recency_rank
        FROM scoped_events
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor = 'system'
              AND t.event_at < TIMESTAMP '2025-03-02 09:00:00'
        )
      AND s.actor = 'agent'
)
SELECT
    actor,
    event_type,
    actor_or_unknown,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count,
    SUM(CASE WHEN event_type IN ('escalated', 'created') THEN 1 ELSE 0 END) AS n_event_type_in_escalated_crea,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM without_matches
GROUP BY GROUPING SETS ((actor, event_type, actor_or_unknown), (actor))
ORDER BY actor NULLS LAST, event_type NULLS LAST, actor_or_unknown NULLS LAST
