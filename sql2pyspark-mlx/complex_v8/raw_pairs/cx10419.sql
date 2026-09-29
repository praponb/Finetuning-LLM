WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE score < 57.3
      AND grade_status IN ('FAIL', 'WITHDRAWN')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY enrolled_at ASC NULLS LAST, id), 3) AS enrolled_at_percent_rank,
        NTH_VALUE(score, 2) OVER (PARTITION BY term ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        score - LAG(score) OVER w_all_by_enrolled_at AS score_change,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id,
        LAG(score) OVER (ORDER BY score, id) AS prev_score,
        ROUND(AVG(score) OVER (ORDER BY score, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        DENSE_RANK() OVER w_all_by_enrolled_at AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER w_term_by_enrolled_at, 3) AS enrolled_at_cume_dist
    FROM scoped_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_student_id AS (PARTITION BY student_id), w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at ASC NULLS LAST, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_share_of_student_id,
        prev_score,
        score_avg_2_prior,
        enrolled_at_dense_rank,
        enrolled_at_cume_dist,
        MAX(second_score) OVER w_term AS term_max_second_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term, student_id ORDER BY prev_score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_term_student_id_by_prev_scor_2, 3) AS prev_score_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score, id), 3) AS score_cume_dist,
        COUNT(*) OVER (PARTITION BY term, student_id ORDER BY prev_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_trends
    WINDOW w_term AS (PARTITION BY term), w_term_student_id_by_prev_scor_2 AS (PARTITION BY term, student_id ORDER BY prev_score DESC)
)
SELECT
    id,
    term,
    score,
    grade_status,
    enrolled_at,
    score_avg_2_prior,
    enrolled_at_cume_dist,
    minutes_since_prev_enrolled_at,
    prev_score_percent_rank,
    score_cume_dist,
    running_count
FROM sequenced
WHERE minutes_since_prev_enrolled_at > 2250.0
ORDER BY enrolled_at DESC, term DESC, id
LIMIT 10
