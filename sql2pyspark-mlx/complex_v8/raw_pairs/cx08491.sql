WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id <= 18
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        actor,
        actor_prefix,
        RANK() OVER (ORDER BY event_at DESC, id) AS event_at_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM base
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        actor,
        actor_prefix,
        last_known_event_at,
        event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER w_second_event_at_month_by_eve, 3) AS event_at_rank_percent_rank,
        event_at_rank - LAG(event_at_rank) OVER (PARTITION BY MONTH(second_event_at) ORDER BY event_at_rank DESC, id) AS event_at_rank_change,
        DENSE_RANK() OVER (PARTITION BY MONTH(second_event_at) ORDER BY event_at_rank DESC) AS event_at_rank_dense_rank,
        ROUND(event_at_cume_dist - AVG(event_at_cume_dist) OVER w_second_event_at_month, 2) AS event_at_cume_dist_vs_second_event_at_month_avg
    FROM ranked
    WINDOW w_second_event_at_month_by_eve AS (PARTITION BY MONTH(second_event_at) ORDER BY event_at_rank DESC), w_second_event_at_month AS (PARTITION BY MONTH(second_event_at))
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked_2
    WHERE event_at_rank_change > (SELECT MIN(event_at_rank_change) FROM ranked_2)
)
-- Subtotals with CUBE
SELECT
    event_at_rank_dense_rank,
    actor,
    COUNT(*) AS cnt,
    COUNT(DISTINCT actor) AS distinct_actors
FROM screened
GROUP BY CUBE(event_at_rank_dense_rank, actor)
