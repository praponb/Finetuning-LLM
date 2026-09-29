WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND grade_status NOT IN ('INCOMPLETE', 'WITHDRAWN')
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term NOT IN ('2025T2', '2024T2')
              AND e.score > 8.8
        )
      AND s.score <= 78.8
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM without_matches s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.grade_status = 'INCOMPLETE'
        )
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        score,
        grade_status,
        ROW_NUMBER() OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at_date, id) AS rn,
        LAG(enrolled_at_date, 2) OVER (PARTITION BY course_id ORDER BY score, id) AS prev2_enrolled_at_date,
        MAX(score) OVER w_enrolled_at_date_year_studen AS enrolled_at_date_year_student_id_max_score,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at_date), student_id) AS rows_in_enrolled_at_date_year_student_id,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score
    FROM screened
    WINDOW w_enrolled_at_date_year_studen AS (PARTITION BY YEAR(enrolled_at_date), student_id)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    rn,
    ROUND(STDDEV(rows_in_enrolled_at_date_year_student_id), 2) AS stddev_rows_in_enrolled_at_date_year_student_id,
    MAX(last_enrolled_at) AS last_last_enrolled_at
FROM metrics
GROUP BY ROLLUP(grade_status, rn)
ORDER BY grade_status NULLS LAST, rn NULLS LAST
