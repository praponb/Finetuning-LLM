WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    session_id,
    course_id,
    new_session_flag,
    ROUND(100 * COUNT(CASE WHEN term = '2024T1' THEN 1 END) / COUNT(*), 1) AS pct_term_2024t1,
    SUM(new_session_flag) AS total_new_session_flag
FROM sessionized
GROUP BY GROUPING SETS ((session_id, course_id, new_session_flag), (session_id), ())
ORDER BY session_id NULLS LAST, course_id NULLS LAST, new_session_flag NULLS LAST
