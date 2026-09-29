WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 08:00:00'
),
with_prev AS (
    -- Previous enrolled_at_year per term
    SELECT *, LAG(enrolled_at_year) OVER (PARTITION BY term ORDER BY enrolled_at_year, id) AS prev_enrolled_at_year
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at_year IS NULL OR (UNIX_TIMESTAMP(enrolled_at_year) - UNIX_TIMESTAMP(prev_enrolled_at_year)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY term ORDER BY enrolled_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at_year,
    prev_enrolled_at_year,
    new_session_flag,
    session_no,
    ABS(session_no - 4) AS session_no_distance,
    DAYOFWEEK(enrolled_at_year) AS enrolled_at_year_dayofweek
FROM sessionized
ORDER BY enrolled_at_year DESC NULLS FIRST
