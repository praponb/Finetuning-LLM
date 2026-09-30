WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        prev_enrolled_at,
        new_session_flag,
        session_no,
        ROUND(new_session_flag - AVG(new_session_flag) OVER w_new_session_flag, 2) AS new_session_flag_vs_new_session_flag_avg,
        ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(LAG(prev_enrolled_at) OVER (PARTITION BY student_id ORDER BY new_session_flag, id))) / 60, 1) AS minutes_since_prev_prev_enrolled_at
    FROM sessionized
    WINDOW w_new_session_flag AS (PARTITION BY new_session_flag)
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.prev_enrolled_at,
        s.new_session_flag,
        s.session_no,
        s.new_session_flag_vs_new_session_flag_avg,
        s.minutes_since_prev_prev_enrolled_at,
        st.cohort AS student_cohort
    FROM sequenced s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort <> 'C2024'
)
SELECT
    id,
    student_id,
    term,
    grade_status,
    prev_enrolled_at,
    session_no,
    new_session_flag_vs_new_session_flag_avg,
    student_cohort
FROM joined_student
WHERE session_no < 4
ORDER BY grade_status NULLS LAST
