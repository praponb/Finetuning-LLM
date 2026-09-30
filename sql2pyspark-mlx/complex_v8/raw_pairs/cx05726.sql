WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER w_actor_ticket_id_by_event_at_3 AS event_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at), 3) AS event_at_percent_rank
    FROM filtered
    WINDOW w_actor_ticket_id_by_event_at_3 AS (PARTITION BY actor, ticket_id ORDER BY event_at, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(event_at_cume_dist * 1.1, 2) AS event_at_cume_dist_adj,
        event_at_seq + event_at_percent_rank AS event_at_seq_plus_event_at_percent_rank,
        CONCAT(actor_prefix, '#', CAST(id AS STRING)) AS actor_prefix_ref
    FROM sequenced
    WHERE HOUR(second_event_at) BETWEEN 8 AND 11
)
SELECT
    event_type,
    actor,
    ticket_id,
    ROUND(SUM(event_at_cume_dist) / NULLIF(SUM(event_at_seq_plus_event_at_percent_rank), 0), 3) AS event_at_cume_dist_to_event_at_seq_plus_event_at,
    PERCENTILE_APPROX(event_at_percent_rank, 0.5) AS median_event_at_percent_rank,
    COUNT(DISTINCT event_type) AS distinct_event_types
FROM derived
GROUP BY CUBE(event_type, actor, ticket_id)
ORDER BY event_type NULLS LAST, actor NULLS LAST, ticket_id NULLS LAST
