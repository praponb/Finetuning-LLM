WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        su.agent_name
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id), 3) AS ticket_csat_cume_dist,
        COUNT(*) OVER (PARTITION BY ticket_csat) AS rows_in_ticket_csat,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY ticket_csat DESC), 3) AS ticket_csat_cume_dist_2,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id)) AS distinct_event_type_in_ticket_id,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY event_type, ticket_id), 0), 4) AS ticket_csat_share_of_event_type_ticket_id,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_id), 2) AS ticket_csat_vs_ticket_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, ticket_agent_id ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        MAX(ticket_csat) OVER (PARTITION BY event_type, ticket_id) AS event_type_ticket_id_max_ticket_csat,
        DENSE_RANK() OVER w_ticket_csat_by_ticket_csat_t AS ticket_csat_dense_rank,
        MAX(ticket_csat) OVER (PARTITION BY event_type, ticket_agent_id ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_1_prior,
        COUNT(*) OVER (PARTITION BY ticket_id) AS rows_in_ticket_id,
        LAST_VALUE(ticket_csat) OVER (PARTITION BY event_type, ticket_agent_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_csat
    FROM enriched
    WINDOW w_ticket_csat_by_ticket_csat_t AS (PARTITION BY ticket_csat ORDER BY ticket_csat ASC)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        ticket_csat,
        ticket_agent_id,
        agent_name,
        minutes_since_prev_event_at,
        ticket_csat_cume_dist,
        rows_in_ticket_csat,
        ticket_csat_cume_dist_2,
        distinct_event_type_in_ticket_id,
        ticket_csat_share_of_event_type_ticket_id,
        event_at_cume_dist,
        event_type_ticket_id_max_ticket_csat,
        ticket_csat_dense_rank,
        ticket_csat_max_1_prior,
        rows_in_ticket_id,
        last_ticket_csat,
        LEAST(rows_in_ticket_id, event_at_cume_dist) AS least_rows_in_ticket_id_event_at_cume_dist,
        DATEDIFF(DATE '2026-01-01', second_event_at) AS days_since_second_event_at,
        SUBSTRING(TRIM(agent_name), 1, 4) AS agent_name_prefix
    FROM with_windows
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(ticket_csat_max_1_prior, 2) OVER (PARTITION BY distinct_event_type_in_ticket_id, last_ticket_csat ORDER BY ticket_csat_cume_dist DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat_max_1_prior,
        DENSE_RANK() OVER w_distinct_event_type_in_ticke_2 AS ticket_csat_cume_dist_dense_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY ticket_csat_dense_rank)) AS distinct_ticket_id_in_ticket_csat_dense_rank,
        MAX(ticket_csat_max_1_prior) OVER (ORDER BY rows_in_ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_csat_max_1_prior_so_far,
        MIN(last_ticket_csat) OVER (PARTITION BY ticket_id) AS ticket_id_min_last_ticket_csat,
        COUNT(*) OVER (PARTITION BY ticket_csat_dense_rank) AS rows_in_ticket_csat_dense_rank,
        FIRST_VALUE(ticket_csat_max_1_prior) IGNORE NULLS OVER (PARTITION BY distinct_event_type_in_ticket_id, ticket_csat ORDER BY minutes_since_prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_csat_max_1_prior,
        NTH_VALUE(ticket_csat_max_1_prior, 2) OVER (ORDER BY event_type_ticket_id_max_ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat_max_1_prior_2
    FROM labeled
    WINDOW w_distinct_event_type_in_ticke_2 AS (PARTITION BY distinct_event_type_in_ticket_id, last_ticket_csat ORDER BY ticket_csat_cume_dist DESC NULLS FIRST)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    ticket_agent_id,
    agent_name,
    minutes_since_prev_event_at,
    ticket_csat_cume_dist,
    rows_in_ticket_csat,
    ticket_csat_cume_dist_2,
    distinct_event_type_in_ticket_id,
    ticket_csat_share_of_event_type_ticket_id,
    event_at_cume_dist,
    event_type_ticket_id_max_ticket_csat,
    ticket_csat_max_1_prior,
    last_ticket_csat,
    least_rows_in_ticket_id_event_at_cume_dist,
    days_since_second_event_at,
    agent_name_prefix,
    ticket_csat_cume_dist_dense_rank,
    distinct_ticket_id_in_ticket_csat_dense_rank,
    max_ticket_csat_max_1_prior_so_far,
    ticket_id_min_last_ticket_csat,
    rows_in_ticket_csat_dense_rank,
    first_known_ticket_csat_max_1_prior,
    second_ticket_csat_max_1_prior_2,
    CONCAT_WS(' / ', agent_name_prefix, event_type) AS agent_name_prefix_event_type_label,
    ABS(event_at_cume_dist - 0.6) AS event_at_cume_dist_distance
FROM with_trends
WHERE agent_name IS NOT NULL
ORDER BY minutes_since_prev_event_at NULLS LAST
