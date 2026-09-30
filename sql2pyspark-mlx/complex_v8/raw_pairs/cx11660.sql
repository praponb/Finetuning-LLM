WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
),
latest_per_key AS (
    -- Keep the latest row per student_id, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
)
SELECT
    course_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    student_id,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    SUM(CASE WHEN grade_status IS NOT NULL THEN 1 ELSE 0 END) AS n_grade_status_is_not_null,
    COUNT(*) AS cnt
FROM latest_per_key
GROUP BY GROUPING SETS ((course_id, grade_status, student_id), (course_id), (grade_status), ())
ORDER BY course_id NULLS LAST, grade_status NULLS LAST, student_id NULLS LAST
