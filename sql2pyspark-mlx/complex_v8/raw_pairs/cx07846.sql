WITH filtered AS (
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
    WHERE student_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_student_id_by_score, 3) AS score_percent_rank,
        MIN(score) OVER (PARTITION BY TO_DATE(enrolled_at)) AS enrolled_at_to_date_min_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_to_date, 0), 4) AS score_share_of_enrolled_at_to_date,
        NTILE(10) OVER (PARTITION BY student_id ORDER BY score ASC, id) AS score_decile,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at)) AS rows_in_enrolled_at_to_date,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        NTILE(4) OVER (PARTITION BY student_id ORDER BY score ASC, id) AS score_quartile
    FROM filtered
    WINDOW w_student_id_by_score AS (PARTITION BY student_id ORDER BY score ASC, id), w_enrolled_at_to_date AS (PARTITION BY TO_DATE(enrolled_at))
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_percent_rank,
        enrolled_at_to_date_min_score,
        second_score,
        score_share_of_enrolled_at_to_date,
        score_decile,
        last_known_enrolled_at,
        rows_in_enrolled_at_to_date,
        first_enrolled_at,
        score_quartile,
        score_percent_rank + second_score AS score_percent_rank_plus_second_score,
        DATE_TRUNC('MONTH', first_enrolled_at) AS first_enrolled_at_month
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        score_percent_rank,
        enrolled_at_to_date_min_score,
        second_score,
        score_quartile,
        score_percent_rank_plus_second_score,
        MAX(rows_in_enrolled_at_to_date) OVER w_student_id_course_id AS student_id_course_id_max_rows_in_enrolled_at_to_,
        ROUND(AVG(second_score) OVER (PARTITION BY student_id, course_id), 2) AS student_id_course_id_avg_second_score,
        LAG(score_decile, 2) OVER (PARTITION BY score_decile, term ORDER BY score_decile DESC, id) AS prev2_score_decile
    FROM enriched
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id)
)
-- Final projection
SELECT
    id,
    student_id,
    score_percent_rank,
    enrolled_at_to_date_min_score,
    score_percent_rank_plus_second_score,
    student_id_course_id_max_rows_in_enrolled_at_to_,
    CASE WHEN score_percent_rank >= 0.8 THEN 'large' WHEN score_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END AS score_percent_rank_tier,
    ABS(score_quartile - 4) AS score_quartile_distance
FROM ranked
