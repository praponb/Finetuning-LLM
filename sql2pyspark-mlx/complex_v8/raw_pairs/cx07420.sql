WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE student_id >= 12
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_parent_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_filled,
        s.prev_enrolled_at,
        s.is_new_session,
        s.burst_id,
        st.student_name,
        st.major AS student_major
    FROM sessionized s
    JOIN students st ON st.id = s.student_id AND st.major IN ('Bio', 'Math')
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    score_filled,
    is_new_session,
    burst_id,
    student_name,
    DATEDIFF(DATE '2026-01-01', prev_enrolled_at) AS days_since_prev_enrolled_at
FROM with_parent_student
WHERE course_id <= 4
