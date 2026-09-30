WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        ABS(gpa - 2.9) AS gpa_distance,
        ABS(gpa - 3.2) AS gpa_distance_2
    FROM students
    WHERE is_international = TRUE
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        enrolled_on,
        student_name_slug,
        gpa_distance,
        LAG(gpa_distance_2) OVER w_all_by_gpa_distance AS prev_gpa_distance_2,
        ROUND(CUME_DIST() OVER w_is_international_by_enrolled, 3) AS enrolled_on_cume_dist
    FROM base_students
    WINDOW w_all_by_gpa_distance AS (ORDER BY gpa_distance, id), w_is_international_by_enrolled AS (PARTITION BY is_international ORDER BY enrolled_on)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE term NOT IN ('2024T1', '2024T2')) AS enrollment_term_count
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM ranked s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name_slug,
        s.prev_gpa_distance_2,
        s.enrolled_on_cume_dist,
        s.enrollment_term_count,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM with_stats s
    LEFT JOIN enrollments e ON e.student_id = s.id
),
enrollment_course_id_stats AS (
    -- Median enrolled_on_cume_dist per enrollment_course_id
    SELECT enrollment_course_id, PERCENTILE_APPROX(enrolled_on_cume_dist, 0.5) AS median_enrolled_on_cume_dist
    FROM with_children
    GROUP BY enrollment_course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_enrolled_on_cume_dist,
        ROUND(s.enrolled_on_cume_dist - g.median_enrolled_on_cume_dist, 2) AS enrolled_on_cume_dist_minus_median
    FROM with_children s
    JOIN enrollment_course_id_stats g ON g.enrollment_course_id = s.enrollment_course_id
    WHERE s.enrolled_on_cume_dist >= g.median_enrolled_on_cume_dist
)
-- Final projection
SELECT
    id,
    prev_gpa_distance_2,
    enrolled_on_cume_dist,
    enrollment_term_count,
    enrollment_id,
    enrollment_course_id,
    enrollment_score,
    median_enrolled_on_cume_dist,
    enrolled_on_cume_dist_minus_median,
    ROUND(enrollment_score / NULLIF(enrollment_term_count, 0), 3) AS enrollment_score_per_enrollment_term_count
FROM above_average
WHERE enrollment_score > 16.3
ORDER BY enrollment_score_per_enrollment_term_count
