WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score >= 38.3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        FIRST_VALUE(score) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term), 0), 4) AS score_share_of_term,
        SUM(score) OVER (PARTITION BY term) AS term_sum_score
    FROM cleaned_enrollments
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        enrolled_at,
        score_share_of_term,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY term_sum_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(AVG(first_score) OVER (PARTITION BY course_id, term ORDER BY term_sum_score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS first_score_avg_3
    FROM with_windows
)
SELECT DISTINCT id, first_score_avg_3
FROM with_trends
ORDER BY id
