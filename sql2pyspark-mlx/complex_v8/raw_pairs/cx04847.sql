WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(TRIM(term)) AS term_clean,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE (score <= 80.2 OR grade_status NOT IN ('PASS', 'FAIL'))
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status NOT IN ('WITHDRAWN', 'FAIL')
        )
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 13
        )
      AND s.student_id <= 7
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_clean,
        enrolled_at_label,
        score_bucket,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY term ORDER BY score_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        score - LAG(score) OVER w_student_id_by_score AS score_change,
        LEAD(score) OVER (PARTITION BY term ORDER BY enrolled_at NULLS LAST, id) AS next_score
    FROM without_matches
    WINDOW w_student_id_by_score AS (PARTITION BY student_id ORDER BY score, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, grade_status, score_bucket
    FROM ranked
) src
PIVOT (
    SUM(score_bucket)
    FOR grade_status IN ('PASS', 'INCOMPLETE', 'FAIL', 'WITHDRAWN')
)
ORDER BY enrolled_at
