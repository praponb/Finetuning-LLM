WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE term = '2024T1'
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score
FROM cleaned_enrollments
WHERE term NOT IN ('2025T1', '2024T2')
ORDER BY id
LIMIT 20
