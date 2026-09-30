WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 19
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.csat >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(ticket_csat) OVER w_ticket_id_event_at_month AS ticket_id_event_at_month_sum_ticket_csat,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_priority, actor), 2) AS ticket_csat_vs_ticket_priority_actor_avg,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id, MONTH(event_at)) AS ticket_id_event_at_month_max_ticket_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY ticket_csat, id), 3) AS ticket_csat_percent_rank,
        LAST_VALUE(ticket_csat) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_csat,
        COUNT(*) OVER (PARTITION BY ticket_id, MONTH(event_at)) AS rows_in_ticket_id_event_at_month,
        SUM(ticket_csat) OVER (PARTITION BY ticket_priority, actor) AS ticket_priority_actor_sum_ticket_csat,
        ROUND(CUME_DIST() OVER (ORDER BY event_at, id), 3) AS event_at_cume_dist,
        NTILE(5) OVER w_actor_ticket_csat_by_ticket AS ticket_csat_quintile,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_ticket_priority_actor, 0), 4) AS ticket_csat_share_of_ticket_priority_actor,
        MIN(ticket_csat) OVER (PARTITION BY ticket_id, MONTH(event_at)) AS ticket_id_event_at_month_min_ticket_csat,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_id, MONTH(event_at)), 2) AS ticket_csat_vs_ticket_id_event_at_month_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY ticket_csat DESC), 3) AS ticket_csat_cume_dist,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_priority, actor)) AS distinct_ticket_id_in_ticket_priority_actor,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id) AS ticket_csat_dense_rank
    FROM enriched
    WINDOW w_ticket_id_event_at_month AS (PARTITION BY ticket_id, MONTH(event_at)), w_actor_ticket_csat_by_ticket AS (PARTITION BY actor, ticket_csat ORDER BY ticket_csat DESC, id), w_ticket_priority_actor AS (PARTITION BY ticket_priority, actor)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE ticket_csat_dense_rank <= 3
      AND ticket_csat_percent_rank > 0.4
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    ticket_csat,
    ticket_priority,
    ticket_id_event_at_month_sum_ticket_csat,
    ticket_csat_vs_ticket_priority_actor_avg,
    ticket_id_event_at_month_max_ticket_csat,
    last_known_ticket_csat,
    rows_in_ticket_id_event_at_month,
    ticket_priority_actor_sum_ticket_csat,
    event_at_cume_dist,
    ticket_csat_share_of_ticket_priority_actor,
    ticket_id_event_at_month_min_ticket_csat,
    ticket_csat_cume_dist,
    ticket_csat_dense_rank
FROM first_rows
ORDER BY ticket_csat_cume_dist DESC, ticket_csat
