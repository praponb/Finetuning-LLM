WITH scoped_enrollments AS (
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
    WHERE course_id <= 6
      AND HOUR(enrolled_at) BETWEEN 10 AND 15
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LEAD(score) OVER (ORDER BY score, id) AS next_score,
        MIN(score) OVER (PARTITION BY term) AS term_min_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, term ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        MAX(score) OVER w_grade_status_student_id AS grade_status_student_id_max_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS grade_status_student_id_avg_score,
        NTILE(5) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_quintile,
        SUM(score) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_sum_score,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term, grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS score_vs_grade_status_student_id_avg,
        SUM(score) OVER (ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2,
        NTILE(4) OVER w_enrolled_at_month_by_enrolle AS enrolled_at_quartile,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at ASC) AS enrolled_at_dense_rank,
        SUM(score) OVER (ORDER BY score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM scoped_enrollments
    WINDOW w_grade_status_student_id AS (PARTITION BY grade_status, student_id), w_enrolled_at_month_by_enrolle AS (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at ASC, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(first_known_enrolled_at)) / 60, 1) AS minutes_first_known_enrolled_at_to_enrolled_at
    FROM with_trends
    WHERE grade_status IS NOT NULL
)
-- Final projection
SELECT DISTINCT enrolled_at_quartile, score_sum_2, id
FROM enriched
WHERE score_sum_2 BETWEEN 30.0 AND 210.0
ORDER BY id
