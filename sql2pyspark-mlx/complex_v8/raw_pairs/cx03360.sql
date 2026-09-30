WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE score <= 57.6
      AND TO_DATE(enrolled_at) < DATE '2025-03-08'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        term_prefix,
        enrolled_at_hour,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_hour) AS distinct_course_id_in_enrolled_at_hour,
        ROUND(enrolled_at_hour - AVG(enrolled_at_hour) OVER w_enrolled_at_hour, 2) AS enrolled_at_hour_vs_enrolled_at_hour_avg,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at ASC, id) AS prev_enrolled_at,
        COUNT(*) OVER (PARTITION BY enrolled_at_hour) AS rows_in_enrolled_at_hour
    FROM valid
    WINDOW w_enrolled_at_hour AS (PARTITION BY enrolled_at_hour)
)
-- Subtotals with CUBE
SELECT
    student_id,
    enrolled_at_hour,
    distinct_course_id_in_enrolled_at_hour,
    MAX(prev_enrolled_at) AS last_prev_enrolled_at
FROM metrics
GROUP BY CUBE(student_id, enrolled_at_hour, distinct_course_id_in_enrolled_at_hour)
ORDER BY student_id NULLS LAST, enrolled_at_hour NULLS LAST, distinct_course_id_in_enrolled_at_hour NULLS LAST
