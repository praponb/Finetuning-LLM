WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        CASE WHEN score >= 57.8 THEN 'A' WHEN score >= 32.7 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
      AND course_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM scoped_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
course_id_stats AS (
    -- Median score per course_id
    SELECT course_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM summarized_rows
    GROUP BY course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM summarized_rows s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score >= g.median_score
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.grade_status_slug,
        s.last_enrollment_enrolled_at,
        s.enrollment_count,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM compared s
    LEFT JOIN enrollments e ON e.student_id = s.student_id AND e.score <= 83.1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, enrollment_id, field, metric_value
FROM (
    SELECT id, enrollment_id, enrollment_score, score
    FROM with_children_enrollments
) src
UNPIVOT (
    metric_value FOR field IN (enrollment_score, score)
) unp
ORDER BY id, enrollment_id, field
