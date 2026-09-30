WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at NULLS LAST, id) AS rn
    FROM sessions
    QUALIFY rn = 1
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.country,
        s.rn,
        top1.web_event_duration_sec
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_sessions AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.rn,
        latest.web_event_duration_sec_2,
        latest.web_event_event_time
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec_2, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(web_event_duration_sec_2, 1) OVER (PARTITION BY TO_DATE(web_event_event_time) ORDER BY web_event_duration_sec_2, id) AS next_web_event_duration_sec_2,
        ROW_NUMBER() OVER (ORDER BY web_event_event_time DESC, id) AS event_seq,
        SUM(web_event_duration_sec_2) OVER (ORDER BY web_event_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_web_event_duration_sec_2,
        ROW_NUMBER() OVER (ORDER BY web_event_event_time, id) AS row_seq,
        FIRST_VALUE(web_event_duration_sec_2, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', web_event_event_time) ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_web_event_duration_sec_2
    FROM latest_match_sessions
    WHERE web_event_event_time < '2023-08-01'
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_web_event_duration_sec_2 >= 459 THEN 'LARGE' ELSE 'SMALL' END AS first_known_web_event_duration_sec_2_tier,
    DATE_TRUNC('WEEK', web_event_event_time) AS week_start,
    MIN(web_event_event_time) AS started_at,
    MAX(web_event_event_time) AS last_seen,
    MAX(web_event_event_time) AS last_seen_2,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    MIN(row_seq) AS min_row_seq,
    GROUPING_ID() AS grouping_level
FROM metrics
GROUP BY CUBE (CASE WHEN first_known_web_event_duration_sec_2 >= 459 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', web_event_event_time))
HAVING COUNT(DISTINCT id) > 1
ORDER BY avg_row_seq DESC;
