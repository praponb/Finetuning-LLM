WITH cleaned AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa,
        DATE_TRUNC('WEEK', enrolled_on) AS enrolled_on_week,
        CASE WHEN cohort IN ('C2022', 'C2021') THEN 'group_a' WHEN cohort = 'C2023' THEN 'group_b' ELSE 'other' END AS cohort_group,
        UPPER(TRIM(major)) AS major_clean
    FROM students
    WHERE enrolled_on < DATE '2025-03-15'
      AND LENGTH(TRIM(student_name)) >= 8
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.major,
        s.gpa,
        s.cohort_group,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM cleaned s
    LEFT JOIN enrollments e ON e.student_id = s.id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at
    FROM line_level_enrollments s
)
SELECT
    enrollment_grade_status,
    enrollment_course_id,
    enrollment_term,
    ROUND(AVG(gpa), 2) AS avg_gpa,
    ARRAY_SORT(COLLECT_SET(major)) AS major_list
FROM with_counts
GROUP BY ROLLUP(enrollment_grade_status, enrollment_course_id, enrollment_term)
ORDER BY enrollment_grade_status NULLS LAST, enrollment_course_id NULLS LAST, enrollment_term NULLS LAST
