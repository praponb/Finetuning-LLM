WITH base_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        DATE_ADD(enrolled_on, 365) AS enrolled_on_plus_365d,
        CASE WHEN gpa >= 3.2 THEN 'large' WHEN gpa >= 2.1 THEN 'medium' ELSE 'small' END AS gpa_tier,
        ROUND(gpa * 1.07, 2) AS gpa_adj
    FROM students
    WHERE enrolled_on >= DATE '2024-10-09'
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count
    FROM base_students s
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.major,
        s.gpa,
        s.enrolled_on_plus_365d,
        s.gpa_tier,
        s.last_enrollment_enrolled_at,
        s.enrollment_count,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM profiled s
    JOIN enrollments e ON e.student_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_count,
    COALESCE(gpa_tier, 'ALL') AS gpa_tier,
    COALESCE(enrollment_grade_status, 'ALL') AS enrollment_grade_status,
    COUNT(*) FILTER (WHERE major NOT IN ('Art', 'Bio')) AS n_major_not_in_art_bio,
    ROUND(STDDEV(enrollment_count), 2) AS stddev_enrollment_count,
    SUM(enrollment_score) AS total_enrollment_score
FROM detail_rows
GROUP BY GROUPING SETS ((enrollment_count, gpa_tier, enrollment_grade_status), (enrollment_count))
ORDER BY enrollment_count NULLS LAST, gpa_tier NULLS LAST, enrollment_grade_status NULLS LAST
