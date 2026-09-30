WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM prepared s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(event_count) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_count_max_7,
        MAX(event_count) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_count_so_far,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS rn,
        SUM(event_count) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_count_sum_6_prior
    FROM summarized_rows
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(event_count_sum_6_prior) OVER (PARTITION BY actor, event_type ORDER BY event_count_sum_6_prior ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS event_count_sum_6_prior_sum_1_prior,
        LAG(rn) OVER w_actor_event_type_by_event_co_2 AS prev_rn,
        MAX(event_count) OVER (PARTITION BY event_type ORDER BY rn ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_count_max_3_prior,
        ROUND(PERCENT_RANK() OVER w_actor_event_type_by_event_co_2, 3) AS event_count_sum_6_prior_percent_rank,
        COUNT(*) OVER (PARTITION BY actor, event_type ORDER BY event_count_sum_6_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY rn ASC NULLS LAST), 3) AS rn_cume_dist
    FROM metrics
    WINDOW w_actor_event_type_by_event_co_2 AS (PARTITION BY actor, event_type ORDER BY event_count_sum_6_prior ASC, id)
)
SELECT
    id,
    ticket_id,
    event_count,
    event_count_max_7,
    max_event_count_so_far,
    event_count_sum_6_prior,
    event_count_sum_6_prior_sum_1_prior,
    prev_rn,
    running_count,
    rn_cume_dist,
    CASE WHEN rn_cume_dist >= 0.8 THEN 'HIGH' WHEN rn_cume_dist >= 0.5 THEN 'MEDIUM' ELSE 'LOW' END AS rn_cume_dist_tier,
    GREATEST(max_event_count_so_far, event_count) AS greatest_max_event_count_so_far_event_count
FROM ranked
ORDER BY event_count_sum_6_prior DESC
