WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'FAIL') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score BETWEEN 34.2 AND 81.4
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status IN ('INCOMPLETE', 'FAIL')
              AND e.score BETWEEN 34.2 AND 94.7
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY grade_status ORDER BY score DESC NULLS FIRST) AS score_rank,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY course_id, grade_status_group), 2) AS course_id_grade_status_group_avg_days_since_enro,
        RANK() OVER (ORDER BY score DESC) AS score_rank_2,
        DENSE_RANK() OVER (ORDER BY score DESC) AS score_dense_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS seq_no
    FROM with_activity
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        course_id_grade_status_group_avg_days_since_enro,
        score_dense_rank,
        seq_no,
        MIN(score_rank) OVER (PARTITION BY score_rank_2) AS score_rank_2_min_score_rank,
        LAST_VALUE(score_rank) OVER (PARTITION BY score_rank_2 ORDER BY score_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_rank,
        FIRST_VALUE(score) OVER (PARTITION BY student_id ORDER BY score_rank_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        MAX(days_since_enrolled_at) OVER w_score_rank_2 AS score_rank_2_max_days_since_enrolled_at,
        RANK() OVER (PARTITION BY score_rank_2 ORDER BY score_dense_rank DESC, id) AS score_dense_rank_rank
    FROM with_trends
    WINDOW w_score_rank_2 AS (PARTITION BY score_rank_2)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        course_id_grade_status_group_avg_days_since_enro,
        score_dense_rank,
        seq_no,
        score_rank_2_min_score_rank,
        last_score_rank,
        score_dense_rank_rank,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY course_id, last_score_rank), 2) AS seq_no_vs_course_id_last_score_rank_avg,
        RANK() OVER (ORDER BY last_score_rank ASC, id) AS last_score_rank_rank,
        NTH_VALUE(first_score, 2) OVER (ORDER BY last_score_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_score
    FROM sequenced
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        score_dense_rank_rank + last_score_rank_rank AS score_dense_rank_rank_plus_last_score_rank_rank,
        score_dense_rank + second_first_score AS score_dense_rank_plus_second_first_score
    FROM ranked
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    enrolled_at,
    course_id_grade_status_group_avg_days_since_enro,
    score_dense_rank,
    seq_no,
    score_rank_2_min_score_rank,
    last_score_rank,
    score_dense_rank_rank,
    seq_no_vs_course_id_last_score_rank_avg,
    second_first_score
FROM annotated
WHERE student_id >= 14
ORDER BY seq_no_vs_course_id_last_score_rank_avg
