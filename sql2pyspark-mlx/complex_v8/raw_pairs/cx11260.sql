WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        student_id,
        grade_status,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null
    FROM recent_enrollments
    GROUP BY 1, 2
),
joined AS (
    -- Attach student attributes
    SELECT
        s.student_id,
        s.grade_status,
        s.n_grade_status_is_not_null,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        st.major AS student_major
    FROM per_group s
    LEFT JOIN students st ON st.id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    n_grade_status_is_not_null,
    grade_status,
    student_major,
    GROUPING_ID() AS gid,
    MAX(student_enrolled_on) AS last_student_enrolled_on
FROM joined
GROUP BY GROUPING SETS ((n_grade_status_is_not_null, grade_status, student_major), (n_grade_status_is_not_null), ())
ORDER BY n_grade_status_is_not_null NULLS LAST, grade_status NULLS LAST, student_major NULLS LAST
