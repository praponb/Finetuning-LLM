WITH base AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        SUBSTRING(TRIM(cohort), 1, 2) AS cohort_prefix,
        CASE WHEN gpa >= 3.2 THEN 'large' WHEN gpa >= 2.1 THEN 'medium' ELSE 'small' END AS gpa_tier
    FROM students
    WHERE major IN ('Bio', 'CS', 'Biz')
      AND UPPER(TRIM(student_name)) LIKE 'M%'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count
    FROM base s
),
stats AS (
    -- Summarise per group
    SELECT
        enrollment_count,
        SUM(enrollment_count) AS total_enrollment_count,
        COUNT(DISTINCT gpa_tier) AS distinct_gpa_tiers,
        COUNT(*) AS cnt,
        MIN(enrollment_count) AS min_enrollment_count,
        MAX(enrolled_on) AS last_enrolled_on
    FROM with_stats
    WHERE cohort_prefix IS NOT NULL
    GROUP BY enrollment_count
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE total_enrollment_count > (SELECT AVG(total_enrollment_count) FROM stats)
)
-- Final projection
SELECT
    enrollment_count,
    distinct_gpa_tiers,
    cnt,
    min_enrollment_count,
    last_enrolled_on,
    QUARTER(last_enrolled_on) AS last_enrolled_on_quarter
FROM with_activity
WHERE cnt BETWEEN 3 AND 10
ORDER BY last_enrolled_on
