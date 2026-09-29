WITH cleaned_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ROUND(gpa * 0.9, 2) AS gpa_adj,
        LOWER(REGEXP_REPLACE(TRIM(major), ' ', '_')) AS major_slug
    FROM students
    WHERE YEAR(enrolled_on) = 2024
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.major,
        s.is_international,
        s.gpa_adj,
        s.major_slug,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term
    FROM cleaned_students s
    JOIN enrollments e ON e.student_id = s.id AND e.course_id <= 6
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_name,
        s.is_international,
        s.gpa_adj,
        s.major_slug,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_course_id,
        s.enrollment_term,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.course_code
    FROM with_children s
    JOIN courses c ON c.id = s.enrollment_course_id AND c.credits <= 3
    WHERE c.credits BETWEEN 2 AND 4
)
SELECT id, enrollment_id, metric, metric_value
FROM (
    SELECT id, enrollment_id, enrollment_score, gpa_adj
    FROM with_details
) src
UNPIVOT (
    metric_value FOR metric IN (enrollment_score AS enrollment_score_v, gpa_adj AS gpa_adj_v)
) unp
ORDER BY id, enrollment_id, metric
