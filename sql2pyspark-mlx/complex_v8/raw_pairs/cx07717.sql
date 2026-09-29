WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        event_at_week,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTH_VALUE(event_at_week, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor ORDER BY event_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_week,
        LAST_VALUE(event_at_week) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_week,
        ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id) AS event_at_seq,
        ROW_NUMBER() OVER w_actor_event_type_by_event_at_3 AS seq_no,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER w_actor_event_type_by_event_at_4, 3) AS event_at_percent_rank,
        DENSE_RANK() OVER w_event_at_to_date_event_type_2 AS event_at_dense_rank,
        LAST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor ORDER BY event_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_week,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        LEAD(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS next_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id), 3) AS event_at_percent_rank_2,
        NTH_VALUE(event_at_week, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_week_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor ORDER BY event_at_week ASC, id) AS event_at_week_rank
    FROM valid_events
    WINDOW w_actor_event_type_by_event_at_3 AS (PARTITION BY actor, event_type ORDER BY event_at_week ASC, id), w_actor_event_type_by_event_at_4 AS (PARTITION BY actor, event_type ORDER BY event_at DESC), w_event_at_to_date_event_type_2 AS (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        event_at_week,
        second_event_at_week,
        event_at_seq,
        seq_no,
        last_known_event_at_week,
        event_at_percent_rank_2,
        second_event_at_week_2,
        event_at_week_rank,
        YEAR(second_event_at_week) AS second_event_at_week_year
    FROM with_windows
    WHERE MONTH(second_event_at_week) IN (2, 3)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(last_known_event_at_week, 2) OVER (PARTITION BY MONTH(second_event_at_week_2), second_event_at_week_year ORDER BY event_at_seq DESC, id) AS prev2_last_known_event_at_week,
        LEAD(second_event_at_week_year) OVER w_all_by_last_known_event_at_w AS next_second_event_at_week_year,
        LAST_VALUE(last_known_event_at_week) IGNORE NULLS OVER (PARTITION BY event_at_seq ORDER BY second_event_at_week_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_event_at_week,
        LAST_VALUE(event_at_week_rank) OVER (ORDER BY second_event_at_week_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_week_rank,
        ROW_NUMBER() OVER w_all_by_last_known_event_at_w AS seq_no_2,
        ROUND(second_event_at_week_year - AVG(second_event_at_week_year) OVER (PARTITION BY event_at_week_rank, second_event_at_week_year), 2) AS second_event_at_week_year_vs_event_at_week_rank_,
        MAX(seq_no) OVER (PARTITION BY event_at_week_rank, second_event_at_week_year) AS event_at_week_rank_second_event_at_week_year_max,
        FIRST_VALUE(second_event_at_week_year) IGNORE NULLS OVER (PARTITION BY event_at_seq ORDER BY second_event_at_week_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_event_at_week_year
    FROM tagged
    WINDOW w_all_by_last_known_event_at_w AS (ORDER BY last_known_event_at_week, id)
)
SELECT
    id,
    event_at_week,
    seq_no,
    last_known_event_at_week,
    event_at_percent_rank_2,
    second_event_at_week_2,
    second_event_at_week_year,
    prev2_last_known_event_at_week,
    last_known_last_known_event_at_week,
    last_event_at_week_rank,
    seq_no_2,
    event_at_week_rank_second_event_at_week_year_max,
    first_known_second_event_at_week_year,
    ROUND(event_at_percent_rank_2 * 1.07, 2) AS event_at_percent_rank_2_adj
FROM sequenced
WHERE seq_no_2 <= 9
ORDER BY event_at_percent_rank_2, event_at_percent_rank_2_adj DESC
