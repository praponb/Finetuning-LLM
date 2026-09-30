WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATEDIFF(DATE '2025-06-30', enrolled_on) AS days_since_enrolled_on,
        ABS(gpa - 2.7) AS gpa_distance
    FROM students
    WHERE (gpa BETWEEN 2.1 AND 3.9 OR UPPER(TRIM(student_name)) LIKE 'D%')
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.id) AS avg_enrollment_score,
        (SELECT COUNT(CASE WHEN e.grade_status IN ('PASS', 'WITHDRAWN', 'FAIL') THEN 1 END) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_grade_status_count
    FROM prepared s
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        major,
        enrolled_on,
        gpa,
        is_international,
        gpa_distance,
        enrollment_grade_status_count,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on_2,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end
    FROM profiled
    WHERE avg_enrollment_score > 25.0
)
SELECT
    id,
    enrolled_on,
    gpa,
    is_international,
    gpa_distance,
    enrollment_grade_status_count,
    days_since_enrolled_on_2,
    cohort_or_unknown,
    enrolled_on_month_end,
    DATEDIFF(DATE '2026-01-01', enrolled_on_month_end) AS days_since_enrolled_on_month_end,
    LENGTH(TRIM(cohort_or_unknown)) AS cohort_or_unknown_len
FROM labeled
WHERE enrollment_grade_status_count <= 5
ORDER BY id DESC, days_since_enrolled_on_2 DESC NULLS FIRST
