WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 9
      AND actor NOT IN ('agent', 'customer')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(event_type_len - AVG(event_type_len) OVER w_event_type_len, 2) AS event_type_len_vs_event_type_len_avg,
        LEAD(event_type_len, 2) OVER w_event_type_event_at_to_date AS next2_event_type_len,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_event_at_to_date)) / 60, 1) AS minutes_since_prev_event_at,
        COUNT(*) OVER (PARTITION BY actor ORDER BY event_type_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(event_type_len) OVER (PARTITION BY actor, YEAR(event_at) ORDER BY event_type_len DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_type_len_sum_6_prior,
        DENSE_RANK() OVER (PARTITION BY actor, YEAR(event_at) ORDER BY event_type_len DESC NULLS FIRST) AS event_type_len_dense_rank,
        LEAD(event_at, 3) OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at DESC, id) AS next3_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, YEAR(event_at) ORDER BY event_type_len DESC NULLS FIRST, id), 3) AS event_type_len_percent_rank,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_type_len, ticket_id ORDER BY event_type_len ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_4,
        event_type_len - LAG(event_type_len) OVER w_actor_event_at_year_by_event_3 AS event_type_len_change,
        event_type_len - LAG(event_type_len) OVER (PARTITION BY actor ORDER BY event_type_len DESC, id) AS event_type_len_change_2,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_type_len), 2) AS event_type_len_avg_event_type_len,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_type_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        COUNT(*) OVER (PARTITION BY actor, YEAR(event_at) ORDER BY event_type_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2
    FROM base
    WINDOW w_event_type_len AS (PARTITION BY event_type_len), w_event_type_event_at_to_date AS (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at DESC, id), w_actor_event_at_year_by_event_3 AS (PARTITION BY actor, YEAR(event_at) ORDER BY event_type_len DESC NULLS FIRST, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_len,
        event_type_or_unknown,
        event_type_len_dense_rank,
        next3_event_at,
        event_type_len_percent_rank,
        event_type_len_avg_4,
        event_type_len_change_2,
        event_type_len_avg_event_type_len,
        last_known_event_at,
        running_count_2,
        ROUND(event_type_len_avg_4 * 1.07, 2) AS event_type_len_avg_4_adj,
        COALESCE(event_type_len_change, 0) AS event_type_len_change_filled,
        ROUND((UNIX_TIMESTAMP(last_known_event_at) - UNIX_TIMESTAMP(next3_event_at)) / 60, 1) AS minutes_next3_event_at_to_last_known_event_at
    FROM ranked
    WHERE event_type_len_dense_rank > 4
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(event_type_len_avg_4, 2) OVER (ORDER BY event_type_len_change_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_type_len_avg_4,
        DENSE_RANK() OVER (ORDER BY event_type_len_dense_rank DESC NULLS FIRST) AS event_type_len_dense_rank_dense_rank,
        SUM(event_type_len_percent_rank) OVER (PARTITION BY event_type_len_dense_rank, event_type_or_unknown) AS event_type_len_dense_rank_event_type_or_unknown_,
        LEAD(last_known_event_at) OVER (ORDER BY event_type_len_change_2, id) AS next_last_known_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_type_len_change_2, id), 3) AS event_type_len_change_2_cume_dist,
        ROUND(event_type_len_percent_rank / NULLIF(SUM(event_type_len_percent_rank) OVER (PARTITION BY event_type_len_dense_rank, event_type_or_unknown), 0), 4) AS event_type_len_percent_rank_share_of_event_type_,
        LAG(event_type_len_change_2, 1, 0) OVER (ORDER BY event_type_len_dense_rank DESC NULLS FIRST, id) AS prev_event_type_len_change_2
    FROM scored
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_or_unknown,
    next3_event_at,
    event_type_len_percent_rank,
    event_type_len_change_2,
    event_type_len_avg_event_type_len,
    running_count_2,
    event_type_len_avg_4_adj,
    event_type_len_change_filled,
    minutes_next3_event_at_to_last_known_event_at,
    second_event_type_len_avg_4,
    event_type_len_dense_rank_dense_rank,
    next_last_known_event_at,
    event_type_len_change_2_cume_dist,
    event_type_len_percent_rank_share_of_event_type_
FROM with_windows
WHERE prev_event_type_len_change_2 < 10
ORDER BY running_count_2, id
LIMIT 10
