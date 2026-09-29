WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE term <> '2025T2'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(score) OVER (PARTITION BY term, DATE_TRUNC('MONTH', enrolled_at)) AS term_enrolled_at_month_max_score,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(score - AVG(score) OVER w_term_student_id, 2) AS score_vs_term_student_id_avg,
        RANK() OVER (ORDER BY score DESC) AS score_rank,
        NTH_VALUE(score, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LAG(score, 3) OVER (PARTITION BY student_id ORDER BY score NULLS LAST, id) AS prev3_score,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC NULLS FIRST) AS enrolled_at_dense_rank,
        ROUND(AVG(score) OVER w_term_student_id, 2) AS term_student_id_avg_score
    FROM cleaned
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    term_enrolled_at_month_max_score,
    first_enrolled_at,
    score_rank,
    second_score,
    prev3_score,
    enrolled_at_dense_rank
FROM with_trends
WHERE student_id IS NOT NULL
