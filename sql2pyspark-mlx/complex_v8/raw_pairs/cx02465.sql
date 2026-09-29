WITH base_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        level
    FROM courses
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        dept,
        level,
        LENGTH(TRIM(level)) AS level_len
    FROM base_courses
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(level, '#', CAST(id AS STRING)) AS level_ref,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM derived
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.level_len,
        s.level_ref,
        s.dept_slug,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM annotated s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        level_len,
        level_ref,
        dept_slug,
        enrollment_id,
        enrollment_score,
        enrollment_grade_status,
        enrollment_student_id,
        enrollment_term,
        level_len - LAG(level_len) OVER w_level_len_enrollment_term_by AS level_len_change,
        SUM(level_len) OVER (PARTITION BY level_len, enrollment_term ORDER BY enrollment_score DESC, id, enrollment_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS level_len_sum_6_prior,
        NTILE(5) OVER (PARTITION BY level_len, enrollment_term ORDER BY enrollment_score DESC, id, enrollment_id) AS enrollment_score_quintile
    FROM detail_rows
    WINDOW w_level_len_enrollment_term_by AS (PARTITION BY level_len, enrollment_term ORDER BY enrollment_score DESC, id, enrollment_id)
)
-- Final projection
SELECT
    id,
    level_len,
    level_ref,
    dept_slug,
    enrollment_id,
    enrollment_score,
    enrollment_student_id,
    level_len_change,
    level_len_sum_6_prior,
    ROUND(enrollment_score / NULLIF(level_len_change, 0), 3) AS enrollment_score_per_level_len_change,
    CASE WHEN enrollment_term IN ('2024T1', '2024T2') THEN 'group_a' WHEN enrollment_term = '2025T2' THEN 'group_b' ELSE 'other' END AS enrollment_term_group
FROM with_windows
WHERE enrollment_id <= 79
