WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(days_since_event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_event_at_sum_6_prior,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY ticket_id), 0), 4) AS days_since_event_at_share_of_ticket_id
    FROM cleaned_events
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        days_since_event_at_sum_6_prior,
        first_event_at,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        first_event_at + INTERVAL 30 MINUTES AS first_event_at_plus_30m,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM with_windows
    WHERE event_type IN ('escalated', 'assigned')
)
SELECT
    event_type_or_unknown,
    actor_or_unknown,
    ticket_id,
    MAX(event_at) AS last_event_at,
    ROUND(AVG(days_since_event_at_sum_6_prior), 2) AS avg_days_since_event_at_sum_6_prior,
    MIN(first_event_at_plus_30m) AS first_first_event_at_plus_30m
FROM labeled
GROUP BY GROUPING SETS ((event_type_or_unknown, actor_or_unknown, ticket_id), (event_type_or_unknown))
ORDER BY event_type_or_unknown NULLS LAST, actor_or_unknown NULLS LAST, ticket_id NULLS LAST
