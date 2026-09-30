WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 6 AND 8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at) AS event_at_rank,
        LEAD(days_since_event_at) OVER w_actor_event_at_month_by_days AS next_days_since_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor, MONTH(event_at) ORDER BY days_since_event_at, id) AS rn,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY MONTH(event_at), actor), 2) AS days_since_event_at_vs_event_at_month_actor_avg
    FROM base
    WINDOW w_actor_event_at_month_by_days AS (PARTITION BY actor, MONTH(event_at) ORDER BY days_since_event_at, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(event_at) AS event_at_date,
        SUBSTRING(TRIM(event_type_actor_label), 1, 4) AS event_type_actor_label_prefix,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM ranked
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label,
        days_since_event_at,
        event_at_rank,
        next_days_since_event_at,
        days_since_event_at_vs_event_at_month_actor_avg,
        event_at_date,
        event_type_actor_label_prefix,
        event_at_plus_30m
    FROM tagged
    WHERE rn = 1
      AND event_at_date BETWEEN DATE '2024-02-01' AND DATE '2025-10-01'
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    event_at_rank,
    COUNT(*) AS row_count,
    ROUND(STDDEV(event_at_rank), 2) AS stddev_event_at_rank
FROM first_rows
GROUP BY GROUPING SETS ((ticket_id, event_type, event_at_rank), (ticket_id), (event_type), ())
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST, event_at_rank NULLS LAST
