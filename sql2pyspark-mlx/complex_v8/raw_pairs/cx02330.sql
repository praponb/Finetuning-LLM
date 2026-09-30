WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ABS(score - 32.7) AS score_distance,
        CASE WHEN score >= 79.2 THEN 'HIGH' WHEN score >= 32.9 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM prepared
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    score_tier,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM tagged
ORDER BY grade_status_len DESC
