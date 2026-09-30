WITH filtered_enrollments AS (
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
    WHERE student_id IS NOT NULL
      AND grade_status = 'INCOMPLETE'
),
with_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM filtered_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(score) OVER (PARTITION BY course_dept, term ORDER BY course_credits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_max_6_prior,
        LAST_VALUE(course_credits) OVER (PARTITION BY course_credits, course_dept ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits,
        MAX(course_credits) OVER (PARTITION BY term ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_course_credits_so_far,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY student_id, course_dept), 2) AS course_credits_vs_student_id_course_dept_avg,
        DENSE_RANK() OVER (PARTITION BY course_dept, term ORDER BY course_credits) AS course_credits_dense_rank,
        SUM(course_credits) OVER (PARTITION BY student_id, course_dept) AS student_id_course_dept_sum_course_credits,
        RANK() OVER w_all_by_score_ties AS score_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score, id), 3) AS score_cume_dist,
        MAX(score) OVER (PARTITION BY student_id, course_dept) AS student_id_course_dept_max_score,
        score - LAG(score) OVER (PARTITION BY term ORDER BY score, id) AS score_change,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        DENSE_RANK() OVER (PARTITION BY course_credits, course_dept ORDER BY course_credits) AS course_credits_dense_rank_2,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, course_dept), 2) AS score_vs_student_id_course_dept_avg,
        SUM(score) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        MAX(score) OVER (PARTITION BY term ORDER BY course_credits DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_max_1_prior
    FROM with_details
    WINDOW w_all_by_score_ties AS (ORDER BY score)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        score_max_6_prior,
        last_course_credits,
        max_course_credits_so_far,
        course_credits_vs_student_id_course_dept_avg,
        score_cume_dist,
        score_change,
        score_vs_student_id_course_dept_avg,
        running_score,
        FIRST_VALUE(course_credits_dense_rank_2) IGNORE NULLS OVER (PARTITION BY course_level ORDER BY score_max_6_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_course_credits_dense_rank_2,
        NTH_VALUE(score_change, 2) OVER (PARTITION BY course_credits, course_id ORDER BY score_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_change,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY term)) AS distinct_course_dept_in_term,
        NTH_VALUE(score_rank, 2) OVER (PARTITION BY course_level ORDER BY score_max_6_prior ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_rank,
        LAST_VALUE(score_change) IGNORE NULLS OVER (ORDER BY score_cume_dist ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_change,
        ROUND(CUME_DIST() OVER w_course_credits_dense_rank_2, 3) AS course_credits_cume_dist,
        RANK() OVER (PARTITION BY course_credits_dense_rank_2, course_dept ORDER BY course_credits, id) AS course_credits_rank
    FROM ranked
    WINDOW w_course_credits_dense_rank_2 AS (PARTITION BY course_credits_dense_rank_2, course_dept ORDER BY course_credits, id)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    last_course_credits,
    max_course_credits_so_far,
    course_credits_vs_student_id_course_dept_avg,
    score_cume_dist,
    score_change,
    running_score,
    second_score_change,
    second_score_rank,
    course_credits_rank,
    CASE WHEN last_known_score_change >= 70.0 THEN 'HIGH' WHEN last_known_score_change >= -20.0 THEN 'MEDIUM' ELSE 'LOW' END AS last_known_score_change_tier
FROM metrics
ORDER BY score_change NULLS LAST
