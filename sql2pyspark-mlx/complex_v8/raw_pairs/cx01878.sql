WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE course_id >= 10
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        SUM(score) AS total_enrollment_score,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.total_enrollment_score,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM sessionized s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        term_clean,
        prev_enrolled_at,
        new_session_flag,
        session_no,
        enrollment_count,
        enrollment_count_2,
        ROW_NUMBER() OVER (PARTITION BY enrollment_count_2 ORDER BY session_no, id) AS seq_no,
        NTH_VALUE(session_no, 2) OVER (PARTITION BY enrollment_count_2 ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_no,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY TO_DATE(prev_enrolled_at))) AS distinct_grade_status_in_prev_enrolled_at_to_dat,
        FIRST_VALUE(new_session_flag) IGNORE NULLS OVER (PARTITION BY enrollment_count_2 ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_new_session_flag,
        score - LAG(score) OVER (PARTITION BY enrollment_count_2 ORDER BY session_no, id) AS score_change,
        COUNT(*) OVER (PARTITION BY TO_DATE(prev_enrolled_at)) AS rows_in_prev_enrolled_at_to_date
    FROM profiled
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    term_clean,
    prev_enrolled_at,
    new_session_flag,
    session_no,
    enrollment_count,
    enrollment_count_2,
    distinct_grade_status_in_prev_enrolled_at_to_dat,
    first_known_new_session_flag,
    score_change
FROM with_windows
ORDER BY grade_status DESC NULLS FIRST, id
LIMIT 10
