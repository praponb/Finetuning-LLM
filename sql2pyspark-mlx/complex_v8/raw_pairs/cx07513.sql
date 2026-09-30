WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type = 'resolved'
      AND ticket_id IS NOT NULL
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type NOT IN ('assigned', 'escalated')
              AND MONTH(t.event_at) IN (1, 2)
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY event_type ORDER BY days_since_event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_event_at_avg_3_prior,
        NTILE(10) OVER w_ticket_id_event_at_month_by AS event_at_decile,
        NTH_VALUE(event_at_hour, 2) OVER (PARTITION BY ticket_id, MONTH(event_at) ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_hour,
        RANK() OVER w_event_type_by_days_since_eve_2 AS days_since_event_at_rank,
        COUNT(*) OVER w_event_type_event_at_hour_yea AS rows_in_event_type_event_at_hour_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY days_since_event_at), 3) AS days_since_event_at_percent_rank
    FROM screened
    WINDOW w_ticket_id_event_at_month_by AS (PARTITION BY ticket_id, MONTH(event_at) ORDER BY event_at DESC NULLS FIRST, id), w_event_type_by_days_since_eve_2 AS (PARTITION BY event_type ORDER BY days_since_event_at, id), w_event_type_event_at_hour_yea AS (PARTITION BY event_type, YEAR(event_at_hour))
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_slug,
    days_since_event_at,
    days_since_event_at_avg_3_prior,
    event_at_decile,
    second_event_at_hour,
    days_since_event_at_rank,
    days_since_event_at_percent_rank,
    CONCAT_WS(' / ', actor_slug, actor) AS actor_slug_actor_label
FROM windowed
WHERE event_at_decile BETWEEN 1 AND 8
ORDER BY id DESC NULLS FIRST, ticket_id NULLS LAST
