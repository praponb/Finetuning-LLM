WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM sessionized s
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    term_group,
    prev_enrolled_at,
    first_enrollment_enrolled_at,
    enrollment_count,
    UPPER(TRIM(term_group)) AS term_group_clean,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM with_stats
