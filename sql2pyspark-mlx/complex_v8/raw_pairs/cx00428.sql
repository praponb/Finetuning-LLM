WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('assigned', 'escalated')
),
with_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.tags AS ticket_tags, t.csat AS ticket_csat
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        actor,
        ticket_tags,
        ROUND(AVG(ticket_csat) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_2,
        SUM(ticket_csat) OVER (ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ticket_csat_sum_2_prior,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_actor_ticket_id, 0), 4) AS ticket_csat_share_of_actor_ticket_id,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_csat, ticket_id), 0), 4) AS ticket_csat_share_of_ticket_csat_ticket_id,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        SUM(ticket_csat) OVER w_actor_ticket_id AS actor_ticket_id_sum_ticket_csat,
        ticket_csat - LAG(ticket_csat) OVER w_all_by_event_at AS ticket_csat_change,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY ticket_csat ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_csat_max_7,
        ROUND(AVG(ticket_csat) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_7,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id) AS ticket_csat_rank,
        FIRST_VALUE(ticket_csat) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_4,
        LAST_VALUE(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_csat
    FROM with_details_ticket
    WINDOW w_actor_ticket_id AS (PARTITION BY actor, ticket_id), w_all_by_event_at AS (ORDER BY event_at, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE ticket_csat_rank <= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY ticket_id ORDER BY ticket_csat_sum_2_prior, id) AS ticket_csat_sum_2_prior_tercile,
        ROUND(ticket_csat_change - AVG(ticket_csat_change) OVER w_ticket_id, 2) AS ticket_csat_change_vs_ticket_id_avg,
        ROW_NUMBER() OVER (PARTITION BY last_ticket_csat, ticket_csat_change ORDER BY minutes_since_prev_event_at, id) AS seq_no,
        LAST_VALUE(ticket_csat_max_7) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY ticket_csat_sum_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_csat_max_7,
        FIRST_VALUE(minutes_since_prev_event_at) IGNORE NULLS OVER (ORDER BY minutes_since_prev_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_minutes_since_prev_event_at
    FROM leaders
    WINDOW w_ticket_id AS (PARTITION BY ticket_id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_csat_sum_2_prior_tercile,
        first_ticket_csat,
        ticket_csat_rank,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) AS cnt,
        COUNT(DISTINCT actor) AS distinct_actors,
        SUM(ticket_csat_avg_2) AS total_ticket_csat_avg_2
    FROM windowed
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        ticket_csat_sum_2_prior_tercile,
        first_ticket_csat,
        ticket_csat_rank,
        actor_list,
        cnt,
        total_ticket_csat_avg_2,
        LEAD(first_ticket_csat) OVER (PARTITION BY ticket_csat_sum_2_prior_tercile ORDER BY total_ticket_csat_avg_2 DESC NULLS FIRST, ticket_csat_sum_2_prior_tercile, first_ticket_csat, ticket_csat_rank) AS next_first_ticket_csat,
        LAST_VALUE(distinct_actors) OVER (PARTITION BY ticket_csat_sum_2_prior_tercile ORDER BY total_ticket_csat_avg_2 DESC NULLS FIRST, ticket_csat_sum_2_prior_tercile, first_ticket_csat, ticket_csat_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_actors,
        MIN(total_ticket_csat_avg_2) OVER (PARTITION BY ticket_csat_sum_2_prior_tercile ORDER BY total_ticket_csat_avg_2 DESC NULLS FIRST, ticket_csat_sum_2_prior_tercile, first_ticket_csat, ticket_csat_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_total_ticket_csat_avg_2_so_far
    FROM per_group
)
SELECT
    ticket_csat_sum_2_prior_tercile,
    first_ticket_csat,
    ticket_csat_rank,
    cnt,
    total_ticket_csat_avg_2,
    next_first_ticket_csat,
    last_distinct_actors,
    LEAST(total_ticket_csat_avg_2, next_first_ticket_csat) AS least_total_ticket_csat_avg_2_next_first_ticket_
FROM with_trends
WHERE ticket_csat_sum_2_prior_tercile > 5
ORDER BY ticket_csat_sum_2_prior_tercile
