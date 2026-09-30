WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score BETWEEN 17.4 AND 77.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term)) AS distinct_course_id_in_term,
        ROUND(days_since_enrolled_at / NULLIF(SUM(days_since_enrolled_at) OVER w_term, 0), 4) AS days_since_enrolled_at_share_of_term,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        FIRST_VALUE(days_since_enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_enrolled_at
    FROM prepared_enrollments
    WINDOW w_term AS (PARTITION BY term)
)
SELECT
    COALESCE(term, 'ALL') AS term,
    student_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(distinct_course_id_in_term), 2) AS avg_distinct_course_id_in_term,
    PERCENTILE_APPROX(distinct_course_id_in_term, 0.5) AS median_distinct_course_id_in_term,
    COUNT(DISTINCT grade_status) AS distinct_grade_status
FROM with_windows
GROUP BY CUBE(term, student_id, grade_status)
ORDER BY term NULLS LAST, student_id NULLS LAST, grade_status NULLS LAST
