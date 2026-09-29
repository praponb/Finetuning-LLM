WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec, 2) AS duration_sec_rounded
    FROM web_events
    WHERE event_time >= TO_DATE('2023-03-27')
      AND session_id IN (12, 21, 33, 35)
),
ranked AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST, id ASC) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
ranked_web_events AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time ASC NULLS LAST, id ASC) = 1
),
with_windows AS (
    SELECT
        id,
        session_id,
        duration_sec,
        SUM(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d,
        NTILE(4) OVER (PARTITION BY session_id ORDER BY duration_sec ASC, id ASC) AS duration_sec_bucket,
        LAG(duration_sec, 1, 0) OVER (ORDER BY event_time ASC, id ASC) AS prev_duration_sec
    FROM ranked_web_events
),
ranked_web_events_2 AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_sum_90d ASC NULLS FIRST, id ASC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
with_dims AS (
    SELECT
        s.*,
        b.device,
        b.country
    FROM ranked_web_events_2 s
    LEFT JOIN sessions b
        ON s.session_id = b.id
)
SELECT
    id,
    duration_sec_sum_90d,
    duration_sec_bucket,
    prev_duration_sec,
    FIRST_VALUE(duration_sec_sum_90d, TRUE) OVER (PARTITION BY country, session_id ORDER BY duration_sec_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_sum_90d,
    SUM(prev_duration_sec) OVER (PARTITION BY country ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_duration_sec
FROM with_dims
WHERE device = 'desktop';
