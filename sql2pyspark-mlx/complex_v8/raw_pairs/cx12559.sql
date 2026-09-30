WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM enrollments
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM recent s
),
with_stats_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status NOT IN ('INCOMPLETE', 'FAIL')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IS NOT NULL) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count_2
    FROM with_stats s
)
SELECT
    student_id,
    enrollment_grade_status_count_2,
    GROUPING(student_id) AS is_total_student_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_stats_2
GROUP BY CUBE(student_id, enrollment_grade_status_count_2)
ORDER BY student_id NULLS LAST, enrollment_grade_status_count_2 NULLS LAST
