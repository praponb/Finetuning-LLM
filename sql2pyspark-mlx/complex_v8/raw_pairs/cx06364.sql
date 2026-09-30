WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE term = '2024T1'
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status_or_unknown,
        s.days_since_enrolled_at,
        c.course_code,
        c.dept AS course_dept
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        days_since_enrolled_at,
        course_dept,
        SUBSTRING(TRIM(course_dept), 1, 3) AS course_dept_prefix,
        CASE WHEN score >= 73.0 THEN 'A' WHEN score >= 34.2 THEN 'B' ELSE 'C' END AS score_tier,
        COALESCE(score, 0.0) AS score_filled
    FROM with_details_course
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_id, score_tier, days_since_enrolled_at
    FROM labeled
) src
PIVOT (
    AVG(days_since_enrolled_at)
    FOR score_tier IN ('A', 'B')
)
ORDER BY student_id
