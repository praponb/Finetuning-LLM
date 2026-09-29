WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        student_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        student_id,
        burst_id,
        session_score,
        DENSE_RANK() OVER (ORDER BY events DESC NULLS FIRST) AS events_dense_rank,
        MIN(burst_id) OVER (ORDER BY events DESC NULLS FIRST, student_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_burst_id_so_far,
        SUM(duration_min) OVER (PARTITION BY student_id) AS student_id_sum_duration_min,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(LAG(session_start) OVER (ORDER BY events DESC NULLS FIRST, student_id, burst_id))) / 60, 1) AS minutes_since_prev_session_start,
        ROUND(CUME_DIST() OVER (ORDER BY events DESC NULLS FIRST), 3) AS events_cume_dist,
        SUM(duration_min) OVER (ORDER BY events DESC NULLS FIRST, student_id, burst_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_min_sum_6_prior
    FROM sessions
),
derived AS (
    -- Label and score rows
    SELECT
        student_id,
        burst_id,
        session_score,
        events_dense_rank,
        student_id_sum_duration_min,
        minutes_since_prev_session_start,
        events_cume_dist,
        duration_min_sum_6_prior,
        ABS(events_dense_rank - 5) AS events_dense_rank_distance,
        ROUND(events_dense_rank / NULLIF(events_cume_dist, 0), 3) AS events_dense_rank_per_events_cume_dist
    FROM with_trends
)
-- Final projection
SELECT
    student_id,
    burst_id,
    session_score,
    events_dense_rank,
    student_id_sum_duration_min,
    minutes_since_prev_session_start,
    events_cume_dist,
    duration_min_sum_6_prior,
    events_dense_rank_distance,
    events_dense_rank_per_events_cume_dist,
    ROUND(events_cume_dist / NULLIF(burst_id, 0), 3) AS events_cume_dist_per_burst_id,
    ROUND(burst_id / NULLIF(events_dense_rank, 0), 3) AS burst_id_per_events_dense_rank
FROM derived
WHERE events_dense_rank_distance >= 4
ORDER BY events_dense_rank_per_events_cume_dist DESC NULLS FIRST
