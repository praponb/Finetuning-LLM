WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        course_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, burst_id
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.course_id,
        s.burst_id,
        s.session_end,
        s.session_score,
        s.duration_min,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM per_session s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
labeled AS (
    -- Label and score rows
    SELECT
        course_id,
        burst_id,
        session_end,
        session_score,
        duration_min,
        enrollment_id,
        enrollment_term,
        enrollment_score,
        enrollment_grade_status,
        DATE_TRUNC('QUARTER', session_end) AS session_end_quarter
    FROM exploded_detail
)
-- Final projection
SELECT
    course_id,
    burst_id,
    session_end,
    session_score,
    duration_min,
    enrollment_id,
    enrollment_term,
    enrollment_grade_status,
    session_end_quarter,
    CASE WHEN enrollment_term IN ('2025T1', '2024T2') THEN 'group_a' WHEN enrollment_term = '2025T2' THEN 'group_b' ELSE 'other' END AS enrollment_term_group
FROM labeled
ORDER BY burst_id DESC
