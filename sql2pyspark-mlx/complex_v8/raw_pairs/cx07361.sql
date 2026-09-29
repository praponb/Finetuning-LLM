WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_type IN ('escalated', 'created', 'assigned')
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_ticket_priority_by_event_at, 3) AS event_at_percent_rank,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY ticket_priority, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_by_ticket_opened_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id))) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        FIRST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY ticket_priority, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_opened_at,
        NTILE(4) OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_quartile,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY ticket_priority, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_priority ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(3) OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_tercile,
        LAG(ticket_opened_at) OVER (PARTITION BY ticket_priority ORDER BY event_at ASC, id) AS prev_ticket_opened_at,
        RANK() OVER w_ticket_priority_by_event_at AS event_at_rank
    FROM with_parent_ticket
    WINDOW w_ticket_priority_by_event_at AS (PARTITION BY ticket_priority ORDER BY event_at ASC), w_actor_by_ticket_opened_at AS (PARTITION BY actor ORDER BY ticket_opened_at DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        ROUND(event_at_percent_rank / NULLIF(event_at_rank, 0), 3) AS event_at_percent_rank_per_event_at_rank,
        ROUND(event_at_percent_rank / NULLIF(ticket_opened_at_tercile, 0), 3) AS event_at_percent_rank_per_ticket_opened_at_terci
    FROM ranked
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', second_ticket_opened_at) AS second_ticket_opened_at_month,
        ticket_opened_at_tercile,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ROUND(AVG(event_at_percent_rank_per_event_at_rank), 2) AS avg_event_at_percent_rank_per_event_at_rank,
        COUNT(*) AS row_count,
        MAX(event_at_percent_rank) AS max_event_at_percent_rank,
        MIN(first_known_ticket_opened_at) AS first_first_known_ticket_opened_at,
        ROUND(AVG(minutes_since_prev_ticket_opened_at), 2) AS avg_minutes_since_prev_ticket_opened_at,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM labeled
    GROUP BY DATE_TRUNC('MONTH', second_ticket_opened_at), ticket_opened_at_tercile, DATE_TRUNC('MONTH', event_at)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        second_ticket_opened_at_month,
        ticket_opened_at_tercile,
        event_at_month,
        avg_event_at_percent_rank_per_event_at_rank,
        max_event_at_percent_rank,
        first_first_known_ticket_opened_at,
        avg_minutes_since_prev_ticket_opened_at,
        distinct_actors,
        FIRST_VALUE(distinct_actors) OVER (PARTITION BY row_count ORDER BY avg_minutes_since_prev_ticket_opened_at ASC, second_ticket_opened_at_month, ticket_opened_at_tercile, event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_actors,
        ROUND(max_event_at_percent_rank - AVG(max_event_at_percent_rank) OVER (PARTITION BY distinct_actors), 2) AS max_event_at_percent_rank_vs_distinct_actors_avg,
        ROUND(max_event_at_percent_rank / NULLIF(SUM(max_event_at_percent_rank) OVER (PARTITION BY distinct_actors), 0), 4) AS max_event_at_percent_rank_share_of_distinct_acto
    FROM per_group
)
SELECT
    second_ticket_opened_at_month,
    event_at_month,
    avg_event_at_percent_rank_per_event_at_rank,
    max_event_at_percent_rank,
    first_first_known_ticket_opened_at,
    avg_minutes_since_prev_ticket_opened_at,
    first_distinct_actors,
    max_event_at_percent_rank_vs_distinct_actors_avg,
    max_event_at_percent_rank_share_of_distinct_acto,
    DATE_TRUNC('YEAR', event_at_month) AS event_at_month_year
FROM metrics
ORDER BY avg_event_at_percent_rank_per_event_at_rank
