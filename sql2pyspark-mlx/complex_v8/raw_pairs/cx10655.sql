WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        QUARTER(event_at) AS event_at_quarter,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE ticket_id >= 45
      AND event_type <> 'resolved'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_slug,
        LAST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(AVG(event_at_quarter) OVER (PARTITION BY ticket_id), 2) AS ticket_id_avg_event_at_quarter,
        ROUND(event_at_quarter - AVG(event_at_quarter) OVER (PARTITION BY ticket_id), 2) AS event_at_quarter_vs_ticket_id_avg,
        MAX(event_at_quarter) OVER (ORDER BY event_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_quarter_max_3,
        LAG(event_at_quarter, 3) OVER (ORDER BY event_at ASC, id) AS prev3_event_at_quarter,
        COUNT(*) OVER (PARTITION BY ticket_id) AS rows_in_ticket_id,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        ROUND(AVG(event_at_quarter) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_at_quarter_avg_1_prior,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id,
        ROW_NUMBER() OVER (PARTITION BY event_at_quarter ORDER BY event_at DESC, id) AS event_at_seq
    FROM scoped
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
derived AS (
    -- Derive helper columns
    SELECT *, COALESCE(prev3_event_at_quarter, 0) AS prev3_event_at_quarter_filled
    FROM windowed
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_actor_in_ticket_id / NULLIF(SUM(distinct_actor_in_ticket_id) OVER w_actor_prev3_event_at_quarter, 0), 4) AS distinct_actor_in_ticket_id_share_of_actor_prev3,
        LAST_VALUE(last_event_at) OVER (ORDER BY event_at_quarter_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, event_at_seq ORDER BY prev3_event_at_quarter ASC, id), 3) AS prev3_event_at_quarter_percent_rank,
        ROUND(event_at_seq / NULLIF(SUM(event_at_seq) OVER (PARTITION BY actor, prev3_event_at_quarter_filled), 0), 4) AS event_at_seq_share_of_actor_prev3_event_at_quart,
        NTH_VALUE(event_at, 2) OVER (ORDER BY rows_in_ticket_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        COUNT(*) OVER (ORDER BY rows_in_ticket_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM derived
    WINDOW w_actor_prev3_event_at_quarter AS (PARTITION BY actor, prev3_event_at_quarter_filled)
)
SELECT
    id,
    ticket_id,
    event_at,
    last_event_at,
    ticket_id_avg_event_at_quarter,
    event_at_quarter_vs_ticket_id_avg,
    event_at_quarter_max_3,
    prev3_event_at_quarter,
    rows_in_ticket_id,
    event_at_quartile,
    event_at_cume_dist,
    event_at_quarter_avg_1_prior,
    distinct_actor_in_ticket_id,
    prev3_event_at_quarter_filled,
    distinct_actor_in_ticket_id_share_of_actor_prev3,
    last_last_event_at,
    prev3_event_at_quarter_percent_rank,
    event_at_seq_share_of_actor_prev3_event_at_quart,
    second_event_at,
    running_count,
    CASE WHEN rows_in_ticket_id >= 16 THEN 'large' WHEN rows_in_ticket_id >= 8 THEN 'medium' ELSE 'small' END AS rows_in_ticket_id_tier,
    CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
FROM with_trends
WHERE prev3_event_at_quarter >= 2
ORDER BY last_event_at, event_at_quartile, id
LIMIT 5
