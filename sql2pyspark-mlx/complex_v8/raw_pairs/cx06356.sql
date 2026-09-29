WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor = 'system'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at_2
    FROM prepared s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_count,
        s.last_event_at_2,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor NOT IN ('system', 'customer')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        last_event_at_2,
        event_id,
        event_event_at,
        event_actor,
        MAX(event_count) OVER (ORDER BY event_count DESC, id, event_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_count_max_3,
        RANK() OVER (PARTITION BY TO_DATE(event_event_at) ORDER BY event_at, id, event_id) AS event_at_rank
    FROM detail_rows
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        last_event_at_2,
        event_id,
        event_event_at,
        event_actor,
        event_count_max_3,
        event_at_rank - event_count_max_3 AS event_at_rank_minus_event_count_max_3,
        ROUND(event_count_max_3 / NULLIF(event_at_rank, 0), 3) AS event_count_max_3_per_event_at_rank,
        GREATEST(event_count_max_3, event_at_rank) AS greatest_event_count_max_3_event_at_rank
    FROM with_windows
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(event_count_max_3) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at_2), MONTH(event_event_at)) AS last_event_at_2_month_event_event_at_month_sum_e,
        LEAD(event_at) OVER w_event_actor_by_event_count_m AS next_event_at
    FROM tagged
    WINDOW w_event_actor_by_event_count_m AS (PARTITION BY event_actor ORDER BY event_count_max_3 DESC, id, event_id)
)
SELECT *
FROM (
    SELECT last_event_at_2, event_actor, event_at_rank_minus_event_count_max_3
    FROM metrics
) src
PIVOT (
    MAX(event_at_rank_minus_event_count_max_3)
    FOR event_actor IN ('customer', 'system', 'agent')
)
ORDER BY last_event_at_2
