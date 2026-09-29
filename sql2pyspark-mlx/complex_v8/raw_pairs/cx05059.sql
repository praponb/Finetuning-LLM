WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(event_type_len) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_type_len_max_2_prior,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY event_type_len, event_type), 0), 4) AS event_type_len_share_of_event_type_len_event_typ,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id) AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at, 3) AS event_at_cume_dist,
        SUM(event_type_len) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_type_len,
        NTILE(4) OVER w_event_type_len_by_event_type AS event_type_len_quartile,
        RANK() OVER w_event_type_len_by_event_type AS event_type_len_rank
    FROM base_events
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id), w_event_type_len_by_event_type AS (PARTITION BY event_type_len ORDER BY event_type_len DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(event_type_len_quartile / NULLIF(event_type_len_rank, 0), 3) AS event_type_len_quartile_per_event_type_len_rank,
        CASE WHEN event_type IN ('assigned', 'replied') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        ABS(running_event_type_len - 85) AS running_event_type_len_distance
    FROM sequenced
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_type_len,
        event_type_len_max_2_prior,
        event_at_cume_dist,
        running_event_type_len,
        event_type_len_quartile,
        event_type_group,
        running_event_type_len_distance,
        DENSE_RANK() OVER w_all_by_running_event_type_le AS running_event_type_len_distance_dense_rank,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_type_group, event_type_len ORDER BY event_type_len_max_2_prior, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_2,
        RANK() OVER (PARTITION BY event_type_group, event_type_len ORDER BY event_type_len_max_2_prior, id) AS event_type_len_max_2_prior_rank,
        NTILE(5) OVER (ORDER BY event_type_len_share_of_event_type_len_event_typ DESC, id) AS event_type_len_share_of_event_type_len_event_typ_2,
        FIRST_VALUE(event_at) OVER (ORDER BY running_event_type_len_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(PERCENT_RANK() OVER w_event_type_group_event_type_3, 3) AS event_type_len_max_2_prior_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY event_type_group, event_type_len ORDER BY event_type_len_max_2_prior, id) AS rn,
        DENSE_RANK() OVER w_all_by_event_type_len_share_2 AS event_type_len_share_of_event_type_len_event_typ_3
    FROM derived
    WINDOW w_all_by_running_event_type_le AS (ORDER BY running_event_type_len_distance ASC, id), w_event_type_group_event_type_3 AS (PARTITION BY event_type_group, event_type_len ORDER BY event_type_len_max_2_prior), w_all_by_event_type_len_share_2 AS (ORDER BY event_type_len_share_of_event_type_len_event_typ DESC)
)
-- Final projection
SELECT
    id,
    event_at,
    actor_prefix,
    event_type_len,
    event_type_len_max_2_prior,
    event_at_cume_dist,
    event_type_len_quartile,
    event_type_group,
    running_event_type_len_distance_dense_rank,
    event_type_len_avg_2,
    event_type_len_max_2_prior_rank,
    event_type_len_share_of_event_type_len_event_typ_2,
    first_event_at,
    event_type_len_max_2_prior_percent_rank,
    rn,
    COALESCE(rn, 0) AS rn_filled,
    SUBSTRING(TRIM(actor_prefix), 1, 4) AS actor_prefix_prefix
FROM with_trends
WHERE event_type_len_max_2_prior_rank >= 2
ORDER BY event_type_len_max_2_prior_rank DESC, id
LIMIT 20
