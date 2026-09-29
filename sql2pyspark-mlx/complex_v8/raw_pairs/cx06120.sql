WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE term IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessionized
    WHERE session_no > (SELECT AVG(session_no) FROM sessionized)
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.term_slug,
        s.prev_enrolled_at,
        s.new_session_flag,
        s.session_no,
        st.student_name,
        st.cohort AS student_cohort,
        st.major AS student_major
    FROM with_activity s
    JOIN students st ON st.id = s.student_id AND st.enrolled_on IS NOT NULL
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    term_slug,
    prev_enrolled_at,
    new_session_flag,
    session_no,
    student_major,
    CASE WHEN session_no >= 4 THEN 'A' WHEN session_no >= 2 THEN 'B' ELSE 'C' END AS session_no_tier,
    DATE_TRUNC('WEEK', prev_enrolled_at) AS prev_enrolled_at_week
FROM enriched
ORDER BY session_no_tier NULLS LAST, prev_enrolled_at
