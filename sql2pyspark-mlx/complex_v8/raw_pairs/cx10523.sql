WITH base AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        gpa,
        is_international,
        CASE WHEN major IN ('Bio', 'CS') THEN 'group_a' WHEN major = 'Math' THEN 'group_b' ELSE 'other' END AS major_group,
        DATE_TRUNC('QUARTER', enrolled_on) AS enrolled_on_quarter,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE LENGTH(TRIM(student_name)) >= 7
      AND enrolled_on < DATE '2025-05-07'
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.gpa,
        s.is_international,
        s.major_group,
        s.is_international_label,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM base s
    JOIN enrollments e ON e.student_id = s.id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.gpa,
        s.major_group,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_course_id,
        e.score AS enrollment_score_2,
        e.enrolled_at AS enrollment_enrolled_at_2,
        st.is_international AS student_is_international
    FROM detail_rows_enrollments s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = e.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_is_international, enrollment_enrolled_at_2, major_group, gpa
    FROM combined_details
) src
PIVOT (
    MIN(gpa) AS min_v, COUNT(gpa) AS n
    FOR major_group IN ('group_b', 'group_a', 'other')
)
