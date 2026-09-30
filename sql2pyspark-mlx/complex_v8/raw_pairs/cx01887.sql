WITH recent_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 10
      AND event_at < TIMESTAMP '2025-02-12 12:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at ASC), 3) AS event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM recent_events
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        second_event_at,
        second_event_at_2,
        minutes_since_prev_event_at,
        MAX(minutes_since_prev_event_at) OVER (ORDER BY minutes_since_prev_event_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minutes_since_prev_event_at_max_3,
        FIRST_VALUE(event_at_percent_rank) IGNORE NULLS OVER (ORDER BY minutes_since_prev_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_percent_rank,
        FIRST_VALUE(event_at_percent_rank) IGNORE NULLS OVER (PARTITION BY event_type, TO_DATE(second_event_at) ORDER BY second_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_percent_rank_2
    FROM with_trends
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(minutes_since_prev_event_at) OVER (PARTITION BY TO_DATE(second_event_at_2), ticket_id ORDER BY first_known_event_at_percent_rank_2 DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS minutes_since_prev_event_at_sum_6_prior,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', second_event_at_2) ORDER BY second_event_at_2 DESC, id))) / 60, 1) AS minutes_since_prev_second_event_at,
        RANK() OVER (ORDER BY minutes_since_prev_event_at_max_3) AS minutes_since_prev_event_at_max_3_rank,
        ROUND(AVG(minutes_since_prev_event_at) OVER w_second_event_at_year, 2) AS second_event_at_year_avg_minutes_since_prev_even,
        FIRST_VALUE(second_event_at_2) OVER (PARTITION BY TO_DATE(second_event_at_2), ticket_id ORDER BY first_known_event_at_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_event_at_2
    FROM windowed
    WINDOW w_second_event_at_year AS (PARTITION BY YEAR(second_event_at))
)
SELECT
    ticket_id,
    minutes_since_prev_event_at_max_3_rank,
    ROUND(AVG(minutes_since_prev_event_at_sum_6_prior), 2) AS avg_minutes_since_prev_event_at_sum_6_prior,
    MAX(minutes_since_prev_event_at) AS max_minutes_since_prev_event_at
FROM metrics
GROUP BY ROLLUP(ticket_id, minutes_since_prev_event_at_max_3_rank)
ORDER BY ticket_id NULLS LAST, minutes_since_prev_event_at_max_3_rank NULLS LAST
