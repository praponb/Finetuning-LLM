WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
)
SELECT 'enrollment' AS source, score_bucket AS ref_id, score AS metric
FROM prepared_enrollments
WHERE grade_status IN ('FAIL', 'WITHDRAWN', 'INCOMPLETE')
  AND TO_DATE(enrolled_at) < DATE '2025-02-12'
UNION ALL
SELECT 'enrollment_2' AS source, student_id AS ref_id, score AS metric
FROM enrollments
