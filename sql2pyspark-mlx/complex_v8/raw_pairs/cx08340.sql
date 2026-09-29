WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE agent_id >= 9
      AND requester_id >= 4
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(csat, 3) OVER w_all_by_opened_at AS next3_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY MONTH(opened_at), csat), 2) AS csat_vs_opened_at_month_csat_avg,
        DENSE_RANK() OVER (PARTITION BY priority ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_dense_rank
    FROM scoped_tickets
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at DESC, id)
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.next3_csat,
        s.csat_vs_opened_at_month_csat_avg,
        s.opened_at_dense_rank,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM ranked s
    JOIN ticket_events t ON t.ticket_id = s.id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        event_id,
        event_at,
        event_actor,
        event_type,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        ROUND(next3_csat / NULLIF(opened_at_dense_rank, 0), 3) AS next3_csat_per_opened_at_dense_rank,
        COALESCE(opened_at_dense_rank, 0) AS opened_at_dense_rank_filled
    FROM line_level_events
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    agent_id,
    event_id,
    COUNT(*) AS cnt,
    SUM(next3_csat_per_opened_at_dense_rank) AS total_next3_csat_per_opened_at_dense_rank,
    MIN(next3_csat_per_opened_at_dense_rank) AS min_next3_csat_per_opened_at_dense_rank
FROM derived
GROUP BY GROUPING SETS ((event_type, agent_id, event_id), (event_type), ())
