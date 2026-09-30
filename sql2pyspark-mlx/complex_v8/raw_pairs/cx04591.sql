WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-01-25 14:00:00' OR event_type IS NOT NULL)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        LAG(event_at) OVER (PARTITION BY actor_group, ticket_id ORDER BY event_at, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (ORDER BY event_at) AS event_at_rank_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor_group, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_group ORDER BY event_at, id), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor_group ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at) OVER w_actor_group_ticket_id_by_eve_3 AS prev_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, actor_group ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROW_NUMBER() OVER w_actor_group_by_event_at AS event_at_seq,
        DENSE_RANK() OVER (PARTITION BY actor, actor_group ORDER BY event_at ASC, id) AS event_at_dense_rank
    FROM scoped
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at NULLS LAST), w_actor_group_ticket_id_by_eve_3 AS (PARTITION BY actor_group, ticket_id ORDER BY event_at DESC, id), w_actor_group_by_event_at AS (PARTITION BY actor_group ORDER BY event_at, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE event_at_seq <= 2
      AND HOUR(prev_event_at_2) BETWEEN 6 AND 10
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_rank,
        event_at_rank_2,
        event_at_percent_rank,
        first_event_at,
        prev_event_at_2,
        second_event_at_2,
        event_at_dense_rank,
        ROUND(event_at_rank_2 - AVG(event_at_rank_2) OVER w_second_event_at_2_year, 2) AS event_at_rank_2_vs_second_event_at_2_year_avg,
        ROUND(event_at_rank / NULLIF(SUM(event_at_rank) OVER (PARTITION BY YEAR(second_event_at_2)), 0), 4) AS event_at_rank_share_of_second_event_at_2_year,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at_rank ASC, id) AS event_at_rank_rank,
        RANK() OVER (ORDER BY event_at_rank_2 DESC, id) AS event_at_rank_2_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at_rank ASC, id), 3) AS event_at_rank_percent_rank,
        ROUND(AVG(event_at_rank_2) OVER (ORDER BY event_at_rank_2 DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_at_rank_2_avg_1_prior,
        FIRST_VALUE(event_at_rank_2) IGNORE NULLS OVER (ORDER BY event_at_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_rank_2,
        ROUND(event_at_seq / NULLIF(SUM(event_at_seq) OVER (PARTITION BY YEAR(second_event_at_2)), 0), 4) AS event_at_seq_share_of_second_event_at_2_year
    FROM first_rows
    WINDOW w_second_event_at_2_year AS (PARTITION BY YEAR(second_event_at_2))
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_rank,
    event_at_rank_2,
    event_at_percent_rank,
    first_event_at,
    prev_event_at_2,
    second_event_at_2,
    event_at_dense_rank,
    event_at_rank_2_vs_second_event_at_2_year_avg,
    event_at_rank_share_of_second_event_at_2_year,
    event_at_rank_rank,
    event_at_rank_2_rank,
    event_at_rank_2_avg_1_prior,
    first_known_event_at_rank_2,
    event_at_seq_share_of_second_event_at_2_year
FROM windowed
