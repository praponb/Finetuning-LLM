WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE course_id <= 4
      AND term IS NOT NULL
),
student_id_stats AS (
    -- Max score per student_id
    SELECT student_id, MAX(score) AS max_score
    FROM filtered_enrollments
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM filtered_enrollments s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score = g.max_score
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM above_average s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_count,
    student_id,
    term,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE term <> '2025T2') AS n_term_2025t2,
    PERCENTILE_APPROX(score, 0.5) AS median_score
FROM with_stats
GROUP BY GROUPING SETS ((enrollment_count, student_id, term), (enrollment_count), (student_id), ())
ORDER BY enrollment_count NULLS LAST, student_id NULLS LAST, term NULLS LAST
