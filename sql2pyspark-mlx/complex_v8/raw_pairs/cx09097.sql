WITH base_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LENGTH(TRIM(level)) AS level_len,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE credits <= 3
      AND level = 'core'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(level)) AS level_len_2,
        level_len + credits AS level_len_plus_credits,
        CONCAT_WS('|', level, dept) AS level_dept_label
    FROM base_courses
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.level_len_2,
        s.level_len_plus_credits,
        s.level_dept_label,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM derived s
    JOIN enrollments e ON e.course_id = s.id AND e.grade_status NOT IN ('WITHDRAWN', 'PASS')
)
SELECT
    id,
    dept,
    level_len_2,
    level_len_plus_credits,
    level_dept_label,
    enrollment_id,
    enrollment_grade_status,
    enrollment_term,
    enrollment_student_id,
    ABS(level_len_2 - 7) AS level_len_2_distance,
    SUBSTRING(TRIM(level_dept_label), 1, 4) AS level_dept_label_prefix
FROM detail_rows_enrollments
WHERE dept IN ('CS', 'Art')
