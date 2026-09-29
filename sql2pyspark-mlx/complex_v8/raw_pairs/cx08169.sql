WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        LENGTH(TRIM(term)) AS term_len,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE student_id >= 16
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_enrolled_at_dayofweek AS rows_in_enrolled_at_dayofweek,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrolled_at_dayofweek, term_len ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        ROUND(enrolled_at_dayofweek / NULLIF(SUM(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek), 0), 4) AS enrolled_at_dayofweek_share_of_enrolled_at_dayof,
        LEAD(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek, term_len ORDER BY enrolled_at, id) AS next_enrolled_at_dayofweek,
        SUM(term_len) OVER (PARTITION BY enrolled_at_dayofweek) AS enrolled_at_dayofweek_sum_term_len
    FROM cleaned
    WINDOW w_enrolled_at_dayofweek AS (PARTITION BY enrolled_at_dayofweek)
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_dayofweek,
        s.term_grade_status_label,
        s.rows_in_enrolled_at_dayofweek,
        s.enrolled_at_percent_rank,
        s.enrolled_at_dayofweek_share_of_enrolled_at_dayof,
        s.next_enrolled_at_dayofweek,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM with_windows s
    JOIN courses c ON c.id = s.course_id
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_details_course s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 11:00:00'
        )
      AND s.student_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    grade_status,
    enrolled_at_dayofweek,
    term_grade_status_label,
    rows_in_enrolled_at_dayofweek,
    enrolled_at_percent_rank,
    enrolled_at_dayofweek_share_of_enrolled_at_dayof,
    next_enrolled_at_dayofweek,
    course_credits,
    course_level,
    course_dept
FROM with_activity
ORDER BY course_level DESC NULLS FIRST, enrolled_at_percent_rank NULLS LAST
