WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        NTILE(3) OVER (ORDER BY event_at DESC, id) AS event_at_tercile,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_ties, 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        LEAD(event_at) OVER (ORDER BY event_at DESC, id) AS next_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM cleaned
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at DESC)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        event_at_tercile,
        event_at_percent_rank,
        event_at_cume_dist,
        second_event_at,
        ROUND(event_at_tercile / NULLIF(SUM(event_at_tercile) OVER (PARTITION BY event_at_tercile, TO_DATE(event_at)), 0), 4) AS event_at_tercile_share_of_event_at_tercile_event,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER w_event_at_tercile_event_at_to, 0), 4) AS event_at_cume_dist_share_of_event_at_tercile_eve,
        FIRST_VALUE(event_at_percent_rank) IGNORE NULLS OVER (ORDER BY event_at_tercile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_percent_rank
    FROM with_windows
    WINDOW w_event_at_tercile_event_at_to AS (PARTITION BY event_at_tercile, TO_DATE(event_at))
),
latest AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        event_at,
        event_type,
        event_at_tercile,
        event_at_percent_rank,
        event_at_cume_dist,
        second_event_at,
        event_at_tercile_share_of_event_at_tercile_event,
        event_at_cume_dist_share_of_event_at_tercile_eve,
        first_known_event_at_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_tercile, id) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(event_at_tercile, 1, 0) OVER (PARTITION BY event_at_tercile ORDER BY event_at ASC, id) AS prev_event_at_tercile,
        LAST_VALUE(second_event_at) OVER (PARTITION BY YEAR(second_event_at), event_at_tercile ORDER BY event_at_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_event_at,
        DENSE_RANK() OVER w_second_event_at_month_by_eve AS event_at_percent_rank_dense_rank,
        LEAD(event_at_percent_rank) OVER (PARTITION BY event_at_tercile ORDER BY event_at ASC, id) AS next_event_at_percent_rank
    FROM latest
    WINDOW w_second_event_at_month_by_eve AS (PARTITION BY MONTH(second_event_at) ORDER BY event_at_percent_rank)
)
SELECT
    id,
    event_at_tercile,
    event_at_percent_rank,
    event_at_cume_dist,
    second_event_at,
    event_at_cume_dist_share_of_event_at_tercile_eve,
    first_known_event_at_percent_rank,
    prev_event_at_tercile,
    last_second_event_at,
    event_at_percent_rank_dense_rank,
    next_event_at_percent_rank,
    event_at_cume_dist - prev_event_at_tercile AS event_at_cume_dist_minus_prev_event_at_tercile,
    DATE_FORMAT(last_second_event_at, 'yyyy-MM-dd HH:00') AS last_second_event_at_label
FROM with_trends_2
ORDER BY event_at_cume_dist DESC NULLS FIRST, id
LIMIT 5
