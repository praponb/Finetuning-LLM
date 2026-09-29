WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        DATE_TRUNC('YEAR', enrolled_on) AS enrolled_on_year,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'I%'
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base_students s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id >= 3
              AND e.score > 38.1
        )
),
summary AS (
    -- Summarise per group
    SELECT
        cohort,
        ROUND(AVG(is_international_flag), 2) AS avg_is_international_flag,
        SUM(CASE WHEN major = 'CS' THEN 1 ELSE 0 END) AS n_major_cs,
        SUM(is_international_flag) AS total_is_international_flag
    FROM filtered
    GROUP BY cohort
    HAVING MAX(gpa) > 2.0
),
tagged AS (
    -- Add calculated columns
    SELECT
        cohort,
        avg_is_international_flag,
        n_major_cs,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        LENGTH(TRIM(cohort)) AS cohort_len
    FROM summary
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM tagged
    WHERE n_major_cs < (SELECT MAX(n_major_cs) FROM tagged)
)
-- Final projection
SELECT cohort, cohort_or_unknown, cohort_len
FROM with_activity
ORDER BY cohort_or_unknown NULLS LAST
