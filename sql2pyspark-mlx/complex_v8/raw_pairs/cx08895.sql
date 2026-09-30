WITH recent AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        level,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug,
        UPPER(TRIM(course_code)) AS course_code_clean,
        CONCAT_WS(' / ', course_code, dept) AS course_code_dept_label
    FROM courses
    WHERE credits >= 2
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code_slug,
        s.course_code_dept_label,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM recent s
    JOIN enrollments e ON e.course_id = s.id
)
SELECT
    enrollment_student_id,
    enrollment_id,
    GROUPING(enrollment_student_id) AS is_total_enrollment_student_id,
    COUNT(DISTINCT enrollment_id) AS distinct_enrollment_ids,
    MAX(enrollment_score) AS max_enrollment_score
FROM exploded_detail_enrollments
GROUP BY GROUPING SETS ((enrollment_student_id, enrollment_id), (enrollment_student_id), (enrollment_id), ())
