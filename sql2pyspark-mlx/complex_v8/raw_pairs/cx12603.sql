WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-01-31 13:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_enrolled_at AS enrolled_at_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id, grade_status), 0), 4) AS score_share_of_course_id_grade_status,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY score, id) AS score_change,
        RANK() OVER (PARTITION BY course_id ORDER BY score) AS score_rank,
        COUNT(*) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(score) OVER w_term_student_id, 2) AS term_student_id_avg_score,
        ROUND(CUME_DIST() OVER w_term_grade_status_by_enrolle, 3) AS enrolled_at_cume_dist,
        MAX(score) OVER (PARTITION BY course_id, grade_status) AS course_id_grade_status_max_score
    FROM base_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC, id), w_term_student_id AS (PARTITION BY term, student_id), w_term_grade_status_by_enrolle AS (PARTITION BY term, grade_status ORDER BY enrolled_at)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        enrolled_at,
        grade_status_ref,
        enrolled_at_dense_rank,
        score_share_of_course_id_grade_status,
        score_change,
        running_count,
        term_student_id_avg_score,
        enrolled_at_cume_dist,
        course_id_grade_status_max_score,
        LOWER(TRIM(term)) AS term_clean,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        running_count - enrolled_at_dense_rank AS running_count_minus_enrolled_at_dense_rank
    FROM metrics
    WHERE term = '2025T1'
)
-- Final projection
SELECT
    id,
    enrolled_at,
    grade_status_ref,
    enrolled_at_dense_rank,
    score_share_of_course_id_grade_status,
    score_change,
    running_count,
    term_student_id_avg_score,
    course_id_grade_status_max_score,
    term_clean,
    running_count_minus_enrolled_at_dense_rank,
    DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
FROM annotated
ORDER BY score_share_of_course_id_grade_status DESC
