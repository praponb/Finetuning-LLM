WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at ASC, id), 3) AS event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_actor_by_ticket_csat, 3) AS ticket_csat_percent_rank,
        SUM(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_2,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_event_at_to_date, 0), 4) AS ticket_csat_share_of_event_at_to_date,
        SUM(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_csat_2,
        LAG(ticket_csat, 3) OVER (PARTITION BY actor ORDER BY ticket_csat ASC, id) AS prev3_ticket_csat
    FROM combined_details
    WINDOW w_actor_by_ticket_csat AS (PARTITION BY actor ORDER BY ticket_csat ASC, id), w_event_at_to_date AS (PARTITION BY TO_DATE(event_at))
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_opened_at,
        ticket_csat,
        distinct_actor_in_ticket_id,
        event_at_percent_rank,
        ticket_csat_avg_2,
        ticket_csat_share_of_event_at_to_date,
        running_ticket_csat_2,
        prev3_ticket_csat,
        GREATEST(event_at_percent_rank, ticket_csat_percent_rank) AS greatest_event_at_percent_rank_ticket_csat_perce
    FROM sequenced
    WHERE ticket_csat_share_of_event_at_to_date BETWEEN 0.3 AND 0.8
),
stats AS (
    -- Summarise per group
    SELECT
        ticket_id,
        actor,
        MIN(ticket_opened_at) AS first_ticket_opened_at,
        ROUND(SUM(greatest_event_at_percent_rank_ticket_csat_perce) / NULLIF(SUM(prev3_ticket_csat), 0), 3) AS greatest_event_at_percent_rank_ticket_csat_perce_2,
        ROUND(SUM(running_ticket_csat_2) / NULLIF(SUM(prev3_ticket_csat), 0), 3) AS running_ticket_csat_2_to_prev3_ticket_csat_ratio,
        SUM(ticket_csat_avg_2) AS total_ticket_csat_avg_2,
        PERCENTILE_APPROX(ticket_csat_avg_2, 0.5) AS median_ticket_csat_avg_2
    FROM labeled
    GROUP BY ticket_id, actor
    HAVING MAX(greatest_event_at_percent_rank_ticket_csat_perce) > 0.3
)
SELECT
    ticket_id,
    actor,
    first_ticket_opened_at,
    greatest_event_at_percent_rank_ticket_csat_perce_2,
    running_ticket_csat_2_to_prev3_ticket_csat_ratio,
    total_ticket_csat_avg_2,
    median_ticket_csat_avg_2,
    ABS(median_ticket_csat_avg_2 - 3.0) AS median_ticket_csat_avg_2_distance
FROM stats
