WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_event_type_label,
        event_at_label,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist
    FROM scoped
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_at_rank,
        ROUND(SUM(event_at_rank) / NULLIF(SUM(event_at_cume_dist), 0), 3) AS event_at_rank_to_event_at_cume_dist_ratio,
        SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer,
        COUNT(*) AS n_rows,
        MAX(event_at_rank) AS max_event_at_rank,
        SUM(event_at_cume_dist) FILTER (WHERE actor = 'agent') AS event_at_cume_dist_actor_agent
    FROM with_trends
    GROUP BY event_at_rank
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE max_event_at_rank > (SELECT AVG(max_event_at_rank) FROM rollup_base)
)
-- Subtotals with GROUPING SETS
SELECT
    max_event_at_rank,
    n_rows,
    n_actor_customer,
    ROUND(STDDEV(event_at_cume_dist_actor_agent), 2) AS stddev_event_at_cume_dist_actor_agent,
    SUM(n_actor_customer) AS total_n_actor_customer,
    ROUND(STDDEV(n_actor_customer), 2) AS stddev_n_actor_customer
FROM filtered
GROUP BY GROUPING SETS ((max_event_at_rank, n_rows, n_actor_customer), (max_event_at_rank), ())
ORDER BY max_event_at_rank NULLS LAST, n_rows NULLS LAST, n_actor_customer NULLS LAST
