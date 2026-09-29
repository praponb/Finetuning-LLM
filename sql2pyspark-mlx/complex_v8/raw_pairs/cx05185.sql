WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score <= 57.8
      AND term = '2025T2'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_id ORDER BY score, id), 3) AS score_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        MAX(score) OVER (PARTITION BY grade_status, term ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        ROUND(AVG(score) OVER (PARTITION BY term, student_id ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        RANK() OVER (PARTITION BY term, course_id ORDER BY score) AS score_rank,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        LAST_VALUE(score) OVER (PARTITION BY grade_status, term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        MAX(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_max_score
    FROM cleaned_enrollments
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE score_rank = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score_percent_rank,
    score_share_of_course_id,
    score_max_2,
    score_avg_7,
    score_rank,
    last_score
FROM top_rows
WHERE student_id >= 14
ORDER BY score_avg_7 DESC NULLS FIRST, last_score, id
LIMIT 5
