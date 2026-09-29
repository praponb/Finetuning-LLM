WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ABS(score - 36.6) AS score_distance
    FROM enrollments
    WHERE course_id >= 7
      AND grade_status NOT IN ('FAIL', 'WITHDRAWN')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(score_distance - AVG(score_distance) OVER w_course_id_student_id, 2) AS score_distance_vs_course_id_student_id_avg,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term, course_id ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY term, course_id ORDER BY score_distance) AS score_distance_dense_rank,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY grade_status, course_id ORDER BY score_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(AVG(score_distance) OVER w_course_id_student_id, 2) AS course_id_student_id_avg_score_distance
    FROM scoped
    WINDOW w_course_id_student_id AS (PARTITION BY course_id, student_id)
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT s.*, c.level AS course_level, c.course_code
    FROM sequenced s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'B%'
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.course_code AS course_course_code
    FROM combined_details_course s
    JOIN courses c ON c.id = s.course_id
    WHERE c.course_code IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count, COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM joined_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        grade_status,
        score_distance_vs_course_id_student_id_avg,
        second_enrolled_at,
        course_level,
        course_code,
        course_dept,
        course_course_code,
        enrollment_count,
        enrollment_count_2,
        ROUND(AVG(score_distance_dense_rank) OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_distance_dense_rank_avg_3,
        ROW_NUMBER() OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC, id) AS enrolled_at_seq,
        ROUND(score_distance_dense_rank - AVG(score_distance_dense_rank) OVER w_enrolled_at_month, 2) AS score_distance_dense_rank_vs_enrolled_at_month_a,
        DENSE_RANK() OVER w_course_credits_by_enrolled_a_2 AS enrolled_at_dense_rank,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_credits ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2
    FROM with_stats
    WINDOW w_enrolled_at_month AS (PARTITION BY MONTH(enrolled_at)), w_course_credits_by_enrolled_a_2 AS (PARTITION BY course_credits ORDER BY enrolled_at ASC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, score_distance_dense_rank_vs_enrolled_at_month_a, score_distance_vs_course_id_student_id_avg
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (score_distance_dense_rank_vs_enrolled_at_month_a, score_distance_vs_course_id_student_id_avg)
) unp
ORDER BY id, metric
