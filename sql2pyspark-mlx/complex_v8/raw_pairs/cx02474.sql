WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 14:00:00'
      AND grade_status = 'PASS'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM recent_enrollments
    WHERE course_id IS NOT NULL
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.student_name,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM labeled s
    JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
    WHERE UPPER(TRIM(st.student_name)) LIKE 'J%'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.min_enrollment_score
    FROM combined_details_student s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Subtotals with CUBE
SELECT
    grade_status_len,
    course_id,
    student_cohort,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN student_name IS NOT NULL THEN score ELSE 0 END) AS score_student_name_is_not_null,
    ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
    MIN(score_filled) AS min_score_filled
FROM profiled
GROUP BY CUBE(grade_status_len, course_id, student_cohort)
ORDER BY grade_status_len NULLS LAST, course_id NULLS LAST, student_cohort NULLS LAST
