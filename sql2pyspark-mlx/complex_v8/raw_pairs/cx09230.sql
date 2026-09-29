WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id >= 12
      AND term IN ('2024T2', '2024T1', '2025T2')
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-03-08 08:00:00'
              AND e.grade_status IS NOT NULL
        )
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_grade_status_label,
        s.grade_status_ref,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.course_id
),
enrollment_grade_status_stats AS (
    -- Median score per enrollment_grade_status
    SELECT enrollment_grade_status, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM line_level
    GROUP BY enrollment_grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM line_level s
    JOIN enrollment_grade_status_stats g ON g.enrollment_grade_status = s.enrollment_grade_status
    WHERE s.score >= g.median_score
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_day,
        ARRAY_SORT(COLLECT_SET(enrollment_term)) AS enrollment_term_list,
        ROUND(SUM(median_score) / NULLIF(SUM(score_minus_median), 0), 3) AS median_score_to_score_minus_median_ratio,
        MAX(enrollment_enrolled_at) AS last_enrollment_enrolled_at,
        PERCENTILE_APPROX(median_score, 0.5) AS median_median_score,
        ROUND(AVG(score), 2) AS avg_score
    FROM compared
    WHERE score <= 63.9
    GROUP BY student_id, TO_DATE(enrollment_enrolled_at)
    HAVING AVG(score_minus_median) > -30.0
)
SELECT student_id, enrollment_enrolled_at_day, field, reading
FROM (
    SELECT student_id, enrollment_enrolled_at_day, avg_score, median_score_to_score_minus_median_ratio
    FROM stats
) src
UNPIVOT (
    reading FOR field IN (avg_score AS m_avg_score, median_score_to_score_minus_median_ratio AS median_score_to_score_minus_median_ratio_v)
) unp
ORDER BY student_id, enrollment_enrolled_at_day, field
