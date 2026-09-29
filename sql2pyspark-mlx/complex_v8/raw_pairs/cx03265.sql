WITH filtered_enrollments AS (
    -- Scope the enrollments rows
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
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
grouped AS (
    -- Summarise per group
    SELECT
        new_session_flag,
        course_id,
        grade_status,
        ROUND(STDDEV(session_id), 2) AS stddev_session_id,
        ROUND(SUM(session_id) / NULLIF(SUM(score), 0), 3) AS session_id_to_score_ratio,
        COUNT(*) FILTER (WHERE term = '2025T2') AS n_term_2025t2,
        PERCENTILE_APPROX(new_session_flag, 0.5) AS median_new_session_flag,
        SUM(session_id) AS total_session_id,
        ROUND(SUM(score) / NULLIF(SUM(new_session_flag), 0), 3) AS score_to_new_session_flag_ratio
    FROM sessionized
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
    GROUP BY 1, 2, 3
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.new_session_flag,
        s.course_id,
        s.grade_status,
        s.stddev_session_id,
        s.session_id_to_score_ratio,
        s.median_new_session_flag,
        s.total_session_id,
        s.score_to_new_session_flag_ratio,
        c.course_code
    FROM grouped s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    new_session_flag,
    course_id,
    grade_status,
    stddev_session_id,
    median_new_session_flag,
    score_to_new_session_flag_ratio,
    COALESCE(stddev_session_id, 0.0) AS stddev_session_id_filled,
    SUBSTRING(TRIM(course_code), 1, 4) AS course_code_prefix
FROM combined_details_course
ORDER BY stddev_session_id DESC NULLS FIRST, stddev_session_id_filled
