WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_type = 'created'
),
with_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.agent_id AS ticket_agent_id
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id) AS next2_event_at,
        RANK() OVER w_actor_ticket_id_by_event_at_2 AS event_at_rank,
        NTILE(10) OVER (ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        LAG(event_at) OVER w_event_type_actor_by_event_at AS prev_event_at,
        LEAD(event_at, 2) OVER (ORDER BY event_at DESC, id) AS next2_event_at_2,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist
    FROM with_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id), w_actor_ticket_id_by_event_at_2 AS (PARTITION BY actor, ticket_id ORDER BY event_at ASC), w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(event_at_cume_dist, event_at_dense_rank) AS least_event_at_cume_dist_event_at_dense_rank,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        CONCAT(event_type_clean, '#', CAST(event_at_decile AS STRING)) AS event_type_clean_ref
    FROM with_windows
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(second_event_at, 2) OVER (PARTITION BY event_at_dense_rank, event_at_decile ORDER BY event_at_decile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_event_at,
        ROUND(minutes_since_prev_event_at / NULLIF(SUM(minutes_since_prev_event_at) OVER w_event_at_dense_rank_actor, 0), 4) AS minutes_since_prev_event_at_share_of_event_at_de,
        ROUND(event_at_dense_rank / NULLIF(SUM(event_at_dense_rank) OVER (PARTITION BY event_at_dense_rank, actor), 0), 4) AS event_at_dense_rank_share_of_event_at_dense_rank,
        SUM(event_at_percent_rank) OVER (PARTITION BY actor ORDER BY event_at_decile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_percent_rank,
        RANK() OVER (ORDER BY event_at_rank) AS event_at_rank_rank,
        MIN(event_at_rank) OVER (PARTITION BY event_at_dense_rank, event_at_decile ORDER BY event_at_decile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_event_at_rank_so_far,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_agent_id)) AS distinct_event_type_in_ticket_agent_id,
        ROUND(event_at_rank - AVG(event_at_rank) OVER w_event_at_dense_rank_actor, 2) AS event_at_rank_vs_event_at_dense_rank_actor_avg
    FROM enriched
    WINDOW w_event_at_dense_rank_actor AS (PARTITION BY event_at_dense_rank, actor)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_clean,
    ticket_agent_id,
    second_event_at,
    event_at_decile,
    event_at_percent_rank,
    event_at_dense_rank,
    prev_event_at,
    least_event_at_cume_dist_event_at_dense_rank,
    second_second_event_at,
    minutes_since_prev_event_at_share_of_event_at_de,
    event_at_dense_rank_share_of_event_at_dense_rank,
    running_event_at_percent_rank,
    event_at_rank_rank,
    min_event_at_rank_so_far,
    distinct_event_type_in_ticket_agent_id,
    event_at_rank_vs_event_at_dense_rank_actor_avg
FROM ranked
ORDER BY event_at_decile DESC NULLS FIRST, distinct_event_type_in_ticket_agent_id DESC, id
LIMIT 20
