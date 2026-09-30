WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'replied'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY event_at), 3) AS event_at_percent_rank_2,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at ASC) AS event_at_rank,
        LAG(event_at) OVER (PARTITION BY YEAR(event_at) ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_at_rank,
        actor,
        SUM(rn) AS total_rn,
        COUNT(DISTINCT actor) AS distinct_actors,
        PERCENTILE_APPROX(event_at_percent_rank, 0.5) AS median_event_at_percent_rank,
        ROUND(SUM(event_at_cume_dist) / NULLIF(SUM(event_at_dense_rank), 0), 3) AS event_at_cume_dist_to_event_at_dense_rank_ratio,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_trends
    GROUP BY event_at_rank, actor
    HAVING SUM(event_at_rank) > 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER w_all_by_total_rn AS total_rn_quintile,
        COUNT(*) OVER (PARTITION BY event_at_rank, distinct_actors) AS rows_in_event_at_rank_distinct_actors,
        ROUND(event_at_rank / NULLIF(SUM(event_at_rank) OVER w_event_at_rank_distinct_actor, 0), 4) AS event_at_rank_share_of_event_at_rank_distinct_ac,
        MAX(distinct_actors) OVER (PARTITION BY event_at_rank, distinct_actors) AS event_at_rank_distinct_actors_max_distinct_actor
    FROM grouped
    WINDOW w_all_by_total_rn AS (ORDER BY total_rn DESC, event_at_rank, actor), w_event_at_rank_distinct_actor AS (PARTITION BY event_at_rank, distinct_actors)
)
SELECT
    event_at_rank,
    actor,
    distinct_actors,
    median_event_at_percent_rank,
    event_at_cume_dist_to_event_at_dense_rank_ratio,
    distinct_ids,
    total_rn_quintile,
    rows_in_event_at_rank_distinct_actors,
    event_at_rank_distinct_actors_max_distinct_actor,
    LOWER(TRIM(actor)) AS actor_clean
FROM sequenced
ORDER BY rows_in_event_at_rank_distinct_actors DESC, event_at_rank, actor
LIMIT 10
