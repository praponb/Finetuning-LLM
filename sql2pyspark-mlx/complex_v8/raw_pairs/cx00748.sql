WITH valid_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_label_2,
        DATEDIFF(DATE '2025-12-31', enrolled_on) AS days_since_enrolled_on
    FROM students
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international,
        enrolled_on_label,
        enrolled_on_label_2,
        days_since_enrolled_on,
        LEAST(gpa, days_since_enrolled_on) AS least_gpa_days_since_enrolled_on,
        LEAST(days_since_enrolled_on, gpa) AS least_days_since_enrolled_on_gpa
    FROM valid_students
    WHERE cohort NOT IN ('C2024', 'C2023')
),
per_group AS (
    -- Summarise per group
    SELECT
        is_international,
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        SUM(least_gpa_days_since_enrolled_on) AS total_least_gpa_days_since_enrolled_on,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
        MIN(least_days_since_enrolled_on_gpa) AS min_least_days_since_enrolled_on_gpa,
        COUNT(*) FILTER (WHERE is_international = FALSE) AS n_is_international_false,
        ROUND(AVG(days_since_enrolled_on), 2) AS avg_days_since_enrolled_on
    FROM labeled
    WHERE least_gpa_days_since_enrolled_on > 70.0
    GROUP BY is_international, DATE_TRUNC('MONTH', enrolled_on)
)
SELECT
    is_international,
    enrolled_on_month,
    total_least_gpa_days_since_enrolled_on,
    student_name_list,
    min_least_days_since_enrolled_on_gpa,
    n_is_international_false,
    avg_days_since_enrolled_on,
    ABS(n_is_international_false - 6) AS n_is_international_false_distance
FROM per_group
ORDER BY avg_days_since_enrolled_on DESC NULLS FIRST, n_is_international_false DESC, is_international, enrolled_on_month
LIMIT 10
