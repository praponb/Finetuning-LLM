WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE student_id >= 10
      AND course_id >= 6
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS min_enrollment_score
    FROM cleaned_enrollments s
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        c.level AS course_level,
        st.gpa AS student_gpa
    FROM profiled s
    JOIN courses c ON c.id = s.course_id AND c.dept <> 'Bio'
    LEFT JOIN students st ON st.id = s.student_id
),
totals AS (
    -- Group-level totals
    SELECT
        course_id,
        ROUND(SUM(student_gpa), 2) AS total_student_gpa,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        PERCENTILE_APPROX(student_gpa, 0.5) AS median_student_gpa,
        SUM(CASE WHEN course_level IS NOT NULL THEN 1 ELSE 0 END) AS n_course_level_is_not_null
    FROM combined_details
    GROUP BY course_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, c.dept AS course_dept
    FROM totals s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    course_id,
    total_student_gpa,
    cnt,
    course_level_list,
    median_student_gpa,
    n_course_level_is_not_null,
    course_dept,
    SIZE(course_level_list) AS course_level_list_count,
    SIZE(course_level_list) AS course_level_list_count_2
FROM with_parent
ORDER BY course_level_list_count_2
