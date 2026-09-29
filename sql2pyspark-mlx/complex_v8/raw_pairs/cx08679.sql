WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 58.8 THEN 'HIGH' WHEN score >= 16.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id >= 16
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_ref,
        st.enrolled_on AS student_enrolled_on
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM enriched
GROUP BY GROUPING SETS ((grade_status, course_id), (grade_status), (course_id))
ORDER BY grade_status NULLS LAST, course_id NULLS LAST
