WITH valid_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        SUBSTRING(TRIM(student_name), 1, 3) AS student_name_prefix,
        CONCAT_WS('-', student_name, major) AS student_name_major_label,
        SUBSTRING(TRIM(student_name), 1, 3) AS student_name_prefix_2
    FROM students
    WHERE cohort IS NOT NULL
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.gpa,
        s.student_name_major_label,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM valid_students s
    LEFT JOIN enrollments e ON e.student_id = s.id AND e.term IN ('2024T1', '2025T2', '2024T2')
),
cohort_stats AS (
    -- Median gpa per cohort
    SELECT cohort, PERCENTILE_APPROX(gpa, 0.5) AS median_gpa
    FROM with_children
    GROUP BY cohort
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_gpa, ROUND(s.gpa - g.median_gpa, 2) AS gpa_minus_median
    FROM with_children s
    JOIN cohort_stats g ON g.cohort = s.cohort
    WHERE s.gpa >= g.median_gpa
)
SELECT
    id,
    student_name,
    cohort,
    gpa,
    enrollment_term,
    enrollment_grade_status,
    gpa_minus_median,
    COALESCE(gpa_minus_median, 0.0) AS gpa_minus_median_filled
FROM vs_group
ORDER BY enrollment_term, student_name DESC
