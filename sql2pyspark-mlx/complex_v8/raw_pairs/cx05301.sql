WITH base AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        ABS(credits - 3) AS credits_distance,
        CASE WHEN credits >= 3 THEN 'A' WHEN credits >= 2 THEN 'B' ELSE 'C' END AS credits_tier
    FROM courses
    WHERE credits >= 2
      AND level IS NOT NULL
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        s.credits,
        s.credits_tier,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM base s
    JOIN enrollments e ON e.course_id = s.id AND e.grade_status = 'WITHDRAWN'
),
stats AS (
    -- Summarise per group
    SELECT
        enrollment_id,
        SUM(enrollment_score) AS total_enrollment_score,
        COUNT(*) FILTER (WHERE dept NOT IN ('Biz', 'Math')) AS n_dept_not_in_biz_math,
        ROUND(SUM(enrollment_score), 2) AS total_enrollment_score_2,
        ROUND(AVG(enrollment_score), 2) AS avg_enrollment_score
    FROM detail_rows_enrollments
    GROUP BY enrollment_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.enrollment_id,
        s.total_enrollment_score,
        s.n_dept_not_in_biz_math,
        s.total_enrollment_score_2,
        s.avg_enrollment_score,
        e.course_id AS enrollment_course_id
    FROM stats s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.enrollment_id,
        s.total_enrollment_score,
        s.total_enrollment_score_2,
        s.avg_enrollment_score,
        s.enrollment_course_id,
        e.id AS enrollment_id_2,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM combined_details s
    JOIN enrollments e ON e.course_id = s.enrollment_course_id
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_id,
    enrollment_course_id,
    ARRAY_SORT(COLLECT_SET(enrollment_grade_status)) AS enrollment_grade_status_list,
    SUM(CASE WHEN enrollment_grade_status IS NOT NULL THEN 1 ELSE 0 END) AS n_enrollment_grade_status_is_n,
    COUNT(DISTINCT enrollment_course_id) AS distinct_enrollment_course_ids
FROM line_level_enrollments
GROUP BY GROUPING SETS ((enrollment_id, enrollment_course_id), (enrollment_id), (enrollment_course_id), ())
