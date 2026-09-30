WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND ticket_id <= 28
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_or_unknown,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTILE(5) OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at DESC, id) AS event_at_quintile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        NTILE(4) OVER w_event_type_by_event_at AS event_at_quartile,
        ROUND(PERCENT_RANK() OVER w_event_type_by_event_at_ties, 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_seq,
        NTILE(5) OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS event_at_quintile_2,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_rank
    FROM valid
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(event_at_quintile - AVG(event_at_quintile) OVER (PARTITION BY event_type), 2) AS event_at_quintile_vs_event_type_avg,
        ROUND(event_at_quintile_2 - AVG(event_at_quintile_2) OVER (PARTITION BY event_type), 2) AS event_at_quintile_2_vs_event_type_avg,
        LEAD(last_event_at) OVER (PARTITION BY actor, MONTH(first_known_event_at) ORDER BY event_at_percent_rank, id) AS next_last_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, MONTH(first_known_event_at) ORDER BY event_at_percent_rank), 3) AS event_at_percent_rank_percent_rank,
        ROW_NUMBER() OVER w_event_at_quintile_2_by_event AS event_at_quintile_2_seq,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER (PARTITION BY event_type), 0), 4) AS event_at_cume_dist_share_of_event_type,
        FIRST_VALUE(first_known_event_at) IGNORE NULLS OVER (PARTITION BY actor, event_at_quintile ORDER BY event_at_quartile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_event_at
    FROM sequenced
    WINDOW w_event_at_quintile_2_by_event AS (PARTITION BY event_at_quintile_2 ORDER BY event_at_quintile_2, id)
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    actor_or_unknown,
    event_at_quintile,
    first_known_event_at,
    event_at_quartile,
    event_at_percent_rank,
    event_at_seq,
    event_at_quintile_vs_event_type_avg,
    next_last_event_at,
    event_at_percent_rank_percent_rank,
    event_at_cume_dist_share_of_event_type,
    first_known_first_known_event_at,
    ABS(event_at_percent_rank - 0.7) AS event_at_percent_rank_distance,
    COALESCE(actor_or_unknown, 'UNKNOWN') AS actor_or_unknown_or_unknown
FROM with_trends
ORDER BY actor_or_unknown NULLS LAST
