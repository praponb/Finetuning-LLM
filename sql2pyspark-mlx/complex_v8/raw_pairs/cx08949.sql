WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE score > 10.1
      AND grade_status = 'WITHDRAWN'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_year,
        grade_status_clean,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM scoped_enrollments
    WHERE enrolled_at_week < TIMESTAMP '2025-02-01 09:00:00'
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM labeled
    WHERE enrolled_at_year < (SELECT MAX(enrolled_at_year) FROM labeled)
)
SELECT
    grade_status_or_unknown,
    grade_status,
    GROUPING(grade_status_or_unknown) AS is_total_grade_status_or_unknown,
    ROUND(STDDEV(score), 2) AS stddev_score
FROM filtered
GROUP BY ROLLUP(grade_status_or_unknown, grade_status)
ORDER BY grade_status_or_unknown NULLS LAST, grade_status NULLS LAST
