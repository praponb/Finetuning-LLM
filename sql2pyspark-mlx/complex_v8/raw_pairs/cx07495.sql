WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE grade_status <> 'FAIL'
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_date,
        s.prev_enrolled_at,
        s.burst_id,
        c.dept AS course_dept
    FROM sessionized s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept IN ('Math', 'Biz')
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at_date,
    prev_enrolled_at,
    course_dept,
    YEAR(enrolled_at_date) AS enrolled_at_date_year,
    burst_id - score AS burst_id_minus_score
FROM with_details
