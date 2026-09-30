WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE actor <> 'system'
      AND ticket_id IS NOT NULL
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_at_week,
        t.tags AS ticket_tags,
        su.agent_name
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        SUM(days_since_event_at) OVER (ORDER BY event_at_week, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_event_at_sum_7,
        NTH_VALUE(days_since_event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_event_at,
        COUNT(*) OVER (PARTITION BY YEAR(event_at)) AS rows_in_event_at_year,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at_week DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS days_since_event_at_avg_6_prior,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_event_at_year, 0), 4) AS days_since_event_at_share_of_event_at_year,
        LEAD(event_at_week) OVER w_ticket_id_by_event_at_week AS next_event_at_week,
        LEAD(days_since_event_at, 1, 0) OVER (ORDER BY event_at_week, id) AS next_days_since_event_at,
        MIN(days_since_event_at) OVER (PARTITION BY YEAR(event_at)) AS event_at_year_min_days_since_event_at
    FROM joined_ticket
    WINDOW w_event_at_year AS (PARTITION BY YEAR(event_at)), w_ticket_id_by_event_at_week AS (PARTITION BY ticket_id ORDER BY event_at_week ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT *, COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM with_trends
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_at_week,
    ticket_tags,
    agent_name,
    days_since_event_at_sum_7,
    second_days_since_event_at,
    days_since_event_at_avg_6_prior,
    next_event_at_week,
    event_at_year_min_days_since_event_at,
    event_type_or_unknown,
    ARRAY_JOIN(ARRAY_SORT(ticket_tags), ',') AS ticket_tags_text
FROM annotated
WHERE rows_in_event_at_year BETWEEN 2 AND 13
ORDER BY second_days_since_event_at NULLS LAST, id
LIMIT 5
