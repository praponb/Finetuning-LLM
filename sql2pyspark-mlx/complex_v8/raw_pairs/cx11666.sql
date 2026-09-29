WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 14
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.prev_enrolled_at,
        s.session_no,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM sessionized s
    JOIN students st ON st.id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_enrolled_on, session_no, student_cohort, student_gpa
    FROM with_details
) src
PIVOT (
    COUNT(student_gpa) AS count_v, SUM(student_gpa) AS sum_v
    FOR student_cohort IN ('C2021', 'C2023', 'C2022')
)
ORDER BY student_enrolled_on, session_no
