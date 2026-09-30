WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 58.8 THEN 'A' WHEN score >= 35.8 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM base s
    JOIN enrollments e ON e.course_id = s.course_id AND MONTH(e.enrolled_at) IN (2, 3)
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_student_id,
    enrollment_id,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(enrollment_score), 2) AS total_enrollment_score,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM exploded_detail
GROUP BY GROUPING SETS ((enrollment_student_id, enrollment_id), (enrollment_student_id), ())
ORDER BY enrollment_student_id NULLS LAST, enrollment_id NULLS LAST
