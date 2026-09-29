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
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE (grade_status IS NOT NULL OR score BETWEEN 13.9 AND 59.7)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        NTH_VALUE(score, 2) OVER (PARTITION BY term ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        RANK() OVER (PARTITION BY MONTH(enrolled_at), term ORDER BY score_bucket DESC) AS score_bucket_rank,
        ROUND(score_bucket - AVG(score_bucket) OVER (PARTITION BY student_id), 2) AS score_bucket_vs_student_id_avg,
        MAX(score) OVER (PARTITION BY student_id) AS student_id_max_score,
        SUM(score_bucket) OVER w_student_id AS student_id_sum_score_bucket,
        LAST_VALUE(score_bucket) OVER (PARTITION BY course_id ORDER BY score_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_bucket,
        LAST_VALUE(score_bucket) IGNORE NULLS OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_bucket,
        LAST_VALUE(score) OVER (PARTITION BY MONTH(enrolled_at), term ORDER BY score_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term), 0), 4) AS score_share_of_term
    FROM valid
    WINDOW w_student_id AS (PARTITION BY student_id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM sequenced
    WHERE student_id <= 9
)
-- Final projection
SELECT DISTINCT rows_in_term, score_bucket, id
FROM annotated
ORDER BY rows_in_term DESC, score_bucket
