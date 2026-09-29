WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
),
filtered AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM base_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.level = 'core'
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY score NULLS LAST, id) AS score_decile,
        MAX(score) OVER (ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id,
        LEAD(enrolled_at, 3) OVER (ORDER BY score NULLS LAST, id) AS next3_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        MAX(score) OVER (PARTITION BY student_id) AS student_id_max_score
    FROM filtered
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        e.total_enrollment_score
    FROM with_trends s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_plus_30m,
        score_decile,
        next3_enrolled_at,
        student_id_sum_score,
        student_id_max_score,
        min_enrollment_score,
        total_enrollment_score,
        ABS(total_enrollment_score - 180.0) AS total_enrollment_score_distance
    FROM profiled
),
term_stats AS (
    -- Median student_id_sum_score per term
    SELECT term, PERCENTILE_APPROX(student_id_sum_score, 0.5) AS median_student_id_sum_score
    FROM tagged
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_student_id_sum_score,
        ROUND(s.student_id_sum_score - g.median_student_id_sum_score, 2) AS student_id_sum_score_minus_median
    FROM tagged s
    JOIN term_stats g ON g.term = s.term
    WHERE s.student_id_sum_score >= g.median_student_id_sum_score
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, student_id_sum_score_minus_median, student_id_max_score, total_enrollment_score_distance
    FROM vs_group
) src
UNPIVOT (
    metric_value FOR metric_name IN (student_id_sum_score_minus_median, student_id_max_score, total_enrollment_score_distance)
) unp
