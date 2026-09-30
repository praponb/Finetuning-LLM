WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 6 AND 10
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_event_type_ticket_agent_id_b AS seq_no,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_csat), 3) AS ticket_csat_cume_dist,
        DENSE_RANK() OVER (PARTITION BY ticket_csat, event_type ORDER BY ticket_csat DESC NULLS FIRST, id) AS ticket_csat_dense_rank,
        MIN(ticket_csat) OVER (PARTITION BY ticket_id) AS ticket_id_min_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_3_prior,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_id), 0), 4) AS ticket_csat_share_of_ticket_id,
        DENSE_RANK() OVER w_ticket_csat_by_ticket_csat AS ticket_csat_dense_rank_2,
        NTH_VALUE(ticket_csat, 2) OVER (PARTITION BY event_type ORDER BY ticket_csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id)) AS distinct_event_type_in_ticket_id,
        ROUND(AVG(ticket_csat) OVER w_ticket_id_event_type, 2) AS ticket_id_event_type_avg_ticket_csat,
        ticket_csat - LAG(ticket_csat) OVER (ORDER BY ticket_csat, id) AS ticket_csat_change,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY ticket_csat NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(ticket_csat, 2) OVER (ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat_2
    FROM enriched_ticket
    WINDOW w_event_type_ticket_agent_id_b AS (PARTITION BY event_type, ticket_agent_id ORDER BY ticket_csat, id), w_ticket_csat_by_ticket_csat AS (PARTITION BY ticket_csat ORDER BY ticket_csat NULLS LAST, id), w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(ticket_id_min_ticket_csat) OVER (PARTITION BY second_ticket_csat_2 ORDER BY ticket_csat_avg_3_prior DESC, id) AS prev_ticket_id_min_ticket_csat,
        NTH_VALUE(ticket_csat_dense_rank, 2) OVER (PARTITION BY second_ticket_csat_2 ORDER BY ticket_csat_avg_3_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat_dense_rank,
        SUM(ticket_csat_change) OVER (PARTITION BY ticket_agent_id ORDER BY ticket_csat_dense_rank_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ticket_csat_change_sum_3,
        ROUND(ticket_id_min_ticket_csat / NULLIF(SUM(ticket_id_min_ticket_csat) OVER (PARTITION BY ticket_csat), 0), 4) AS ticket_id_min_ticket_csat_share_of_ticket_csat
    FROM metrics
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_agent_id,
    ticket_csat,
    seq_no,
    ticket_csat_cume_dist,
    ticket_id_min_ticket_csat,
    ticket_csat_share_of_ticket_id,
    ticket_csat_dense_rank_2,
    second_ticket_csat,
    distinct_event_type_in_ticket_id,
    ticket_csat_change,
    minutes_since_prev_event_at,
    second_ticket_csat_2,
    second_ticket_csat_dense_rank,
    ticket_csat_change_sum_3,
    ticket_id_min_ticket_csat_share_of_ticket_csat,
    CASE WHEN ticket_csat_dense_rank >= 9 THEN 'large' WHEN ticket_csat_dense_rank >= 3 THEN 'medium' ELSE 'small' END AS ticket_csat_dense_rank_tier
FROM ranked
