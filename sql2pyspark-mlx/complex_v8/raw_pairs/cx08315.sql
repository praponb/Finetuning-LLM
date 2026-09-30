WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE course_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at ASC, id) AS enrolled_at_rank,
        LAG(enrolled_at) OVER w_grade_status_by_enrolled_at AS prev_enrolled_at,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank_2,
        LAST_VALUE(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at ASC, id), 3) AS enrolled_at_percent_rank_2,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status_or_unknown), 2) AS score_vs_grade_status_or_unknown_avg,
        LAG(enrolled_at) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id) AS prev_enrolled_at_2,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id,
        DENSE_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at ASC, id) AS enrolled_at_dense_rank
    FROM valid
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at ASC, id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at ASC), w_student_id AS (PARTITION BY student_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE enrolled_at_dense_rank <= 5
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(score_share_of_student_id) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_share_of_student_id,
        ROUND(AVG(enrolled_at_percent_rank_2) OVER (PARTITION BY enrolled_at_dense_rank ORDER BY enrolled_at_rank DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrolled_at_percent_rank_2_avg_2_prior,
        NTH_VALUE(prev_enrolled_at_2, 2) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_enrolled_at_2
    FROM top_rows
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at_percent_rank,
    enrolled_at_rank,
    prev_enrolled_at,
    enrolled_at_rank_2,
    last_score,
    score_vs_grade_status_or_unknown_avg,
    prev_enrolled_at_2,
    score_share_of_student_id,
    enrolled_at_dense_rank,
    running_score_share_of_student_id,
    enrolled_at_percent_rank_2_avg_2_prior,
    YEAR(prev_enrolled_at) AS prev_enrolled_at_year
FROM with_trends
WHERE grade_status = 'PASS'
ORDER BY enrolled_at_dense_rank, enrolled_at_rank NULLS LAST, id
LIMIT 5
