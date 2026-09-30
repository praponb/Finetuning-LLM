WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        grade_status_clean,
        DENSE_RANK() OVER w_student_id_term_by_enrolled AS enrolled_at_dense_rank,
        LAG(enrolled_at) OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id) AS prev_enrolled_at,
        NTILE(5) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at) AS enrolled_at_dense_rank_3,
        ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id) AS enrolled_at_seq,
        NTILE(10) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS enrolled_at_decile,
        RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at) AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS rn,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank_2
    FROM recent
    WINDOW w_student_id_term_by_enrolled AS (PARTITION BY student_id, term ORDER BY enrolled_at)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        grade_status_clean,
        enrolled_at_dense_rank,
        prev_enrolled_at,
        enrolled_at_quintile,
        enrolled_at_percent_rank,
        enrolled_at_dense_rank_3,
        enrolled_at_seq,
        enrolled_at_decile,
        enrolled_at_rank,
        last_enrolled_at,
        second_enrolled_at,
        enrolled_at_percent_rank_2
    FROM with_trends
    WHERE rn <= 2
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN enrolled_at_percent_rank >= 0.7 THEN 'large' WHEN enrolled_at_percent_rank >= 0.4 THEN 'medium' ELSE 'small' END AS enrolled_at_percent_rank_tier,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        enrolled_at_seq,
        COUNT(*) FILTER (WHERE grade_status = 'FAIL') AS n_grade_status_fail,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        ROUND(SUM(enrolled_at_percent_rank_2) / NULLIF(SUM(enrolled_at_dense_rank), 0), 3) AS enrolled_at_percent_rank_2_to_enrolled_at_dense_
    FROM top_per_group
    WHERE enrolled_at_rank >= 4
    GROUP BY 1, 2, 3
    HAVING MAX(enrolled_at_dense_rank) > 3
)
SELECT
    enrolled_at_percent_rank_tier,
    enrolled_at_seq,
    n_grade_status_fail,
    SUBSTRING(TRIM(enrolled_at_ym), 1, 4) AS enrolled_at_ym_prefix,
    LENGTH(TRIM(enrolled_at_ym)) AS enrolled_at_ym_len
FROM summary
ORDER BY enrolled_at_ym_len DESC NULLS FIRST
