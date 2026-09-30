WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-24'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_clean,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM recent_enrollments
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    days_since_enrolled_at,
    UPPER(TRIM(enrolled_at_label)) AS enrolled_at_label_clean
FROM labeled
WHERE days_since_enrolled_at >= 105
ORDER BY days_since_enrolled_at DESC NULLS FIRST, id DESC NULLS FIRST
