WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND term IN ('2025T1', '2025T2')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(score) OVER (ORDER BY enrolled_at ASC NULLS LAST, id) AS next_score,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS seq_no,
        ROUND(AVG(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id), 2) AS enrolled_at_month_student_id_avg_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id), 0), 4) AS score_share_of_enrolled_at_month_student_id,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        RANK() OVER w_all_by_score_ties AS score_rank,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id)) AS distinct_course_id_in_enrolled_at_month_student_,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist,
        score - LAG(score) OVER (ORDER BY score, id) AS score_change,
        ROUND(score - AVG(score) OVER w_enrolled_at_month_student_id, 2) AS score_vs_enrolled_at_month_student_id_avg,
        MIN(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id) AS enrolled_at_month_student_id_min_score
    FROM valid_enrollments
    WINDOW w_all_by_score_ties AS (ORDER BY score DESC), w_enrolled_at_month_student_id AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE seq_no <= 5
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    seq_no,
    enrolled_at_month_student_id_avg_score,
    score_share_of_enrolled_at_month_student_id,
    score_rank,
    distinct_course_id_in_enrolled_at_month_student_,
    score_cume_dist,
    score_change,
    score_vs_enrolled_at_month_student_id_avg
FROM top_rows
ORDER BY id
