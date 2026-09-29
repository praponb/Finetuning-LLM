WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor IN ('system', 'agent'))
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        actor,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at ASC, id), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER w_event_type_by_event_at, 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at ASC) AS event_at_rank
    FROM valid
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at ASC, id)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE event_at_rank > (SELECT AVG(event_at_rank) FROM metrics)
),
latest AS (
    -- Keep the latest row per actor
    SELECT
        id,
        actor,
        event_at_percent_rank,
        last_known_event_at,
        event_at_cume_dist,
        event_at_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at_cume_dist DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY event_at_rank, actor) AS rows_in_event_at_rank_actor,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_at_rank ORDER BY last_known_event_at ASC, id), 3) AS last_known_event_at_percent_rank,
        NTH_VALUE(event_at_rank, 2) OVER (PARTITION BY event_at_rank ORDER BY last_known_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_rank,
        ROUND(AVG(event_at_cume_dist) OVER (PARTITION BY event_at_rank, MONTH(last_known_event_at) ORDER BY event_at_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS event_at_cume_dist_avg_2_prior
    FROM latest
)
-- Final projection
SELECT
    id,
    actor,
    event_at_cume_dist,
    rows_in_event_at_rank_actor,
    last_known_event_at_percent_rank,
    second_event_at_rank,
    event_at_cume_dist_avg_2_prior,
    CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
    ABS(second_event_at_rank - 4) AS second_event_at_rank_distance
FROM windowed
WHERE actor IN ('system', 'agent')
ORDER BY last_known_event_at_percent_rank DESC, rows_in_event_at_rank_actor, id
LIMIT 5
