WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        term,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        term,
        session_id,
        session_start,
        session_end,
        n_events,
        duration_min,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', session_end), n_events ORDER BY session_score, term, session_id) AS session_score_dense_rank,
        LAST_VALUE(duration_min) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', session_end), n_events ORDER BY session_score, term, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_min,
        MAX(n_events) OVER (PARTITION BY session_id, n_events ORDER BY session_start ASC NULLS LAST, term, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_events_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY session_id, n_events ORDER BY session_start ASC NULLS LAST), 3) AS session_start_cume_dist
    FROM sessions
)
-- Native SQL UNPIVOT (wide to long)
SELECT term, session_id, metric, reading
FROM (
    SELECT term, session_id, last_known_duration_min, session_start_cume_dist, duration_min
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric IN (last_known_duration_min AS last_known_duration_min_v, session_start_cume_dist AS session_start_cume_dist_v, duration_min AS duration_min_v)
) unp
ORDER BY term, session_id, metric
