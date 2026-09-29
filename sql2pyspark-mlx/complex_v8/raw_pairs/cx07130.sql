WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE major = 'Biz'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(gpa, 0.0) AS gpa_filled,
        CASE WHEN cohort IN ('C2023', 'C2022') THEN 'group_a' WHEN cohort = 'C2021' THEN 'group_b' ELSE 'other' END AS cohort_group,
        SUBSTRING(TRIM(is_international_label), 1, 3) AS is_international_label_prefix
    FROM valid_students
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.enrolled_on,
        s.gpa,
        s.is_international,
        s.is_international_label,
        s.cohort_group,
        s.is_international_label_prefix,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term
    FROM annotated s
    LEFT JOIN enrollments e ON e.student_id = s.id
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM line_level s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
)
SELECT *
FROM (
    SELECT enrollment_count, enrolled_on, cohort, gpa
    FROM profiled
) src
PIVOT (
    SUM(gpa)
    FOR cohort IN ('C2022', 'C2023', 'C2021', 'C2024')
)
ORDER BY enrollment_count, enrolled_on
