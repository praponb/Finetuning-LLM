WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score <= 63.9
      AND course_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        grade_status_group,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status_group, term)) AS distinct_course_id_in_grade_status_group_term,
        COUNT(*) OVER (PARTITION BY term ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(score) OVER (PARTITION BY course_id, grade_status_group ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY score DESC NULLS FIRST) AS score_dense_rank,
        ROUND(AVG(score) OVER (PARTITION BY course_id, grade_status_group ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        NTILE(5) OVER (PARTITION BY grade_status_group ORDER BY enrolled_at DESC, id) AS enrolled_at_quintile,
        MAX(score) OVER (PARTITION BY course_id, grade_status_group ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status_group_term, 0), 4) AS score_share_of_grade_status_group_term,
        LAG(score) OVER (PARTITION BY course_id, grade_status_group ORDER BY enrolled_at DESC, id) AS prev_score,
        SUM(score) OVER (PARTITION BY grade_status_group, term) AS grade_status_group_term_sum_score
    FROM recent
    WINDOW w_grade_status_group_term AS (PARTITION BY grade_status_group, term)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        grade_status_group,
        distinct_course_id_in_grade_status_group_term,
        running_count,
        last_score,
        score_avg_7,
        score_max_2_prior,
        prev_score,
        grade_status_group_term_sum_score,
        NTH_VALUE(enrolled_at_quintile, 2) OVER (ORDER BY running_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrolled_at_quintile, student_id ORDER BY last_score ASC, id), 3) AS last_score_percent_rank,
        SUM(score_avg_7) OVER w_term_student_id AS term_student_id_sum_score_avg_7,
        LAG(running_count) OVER (ORDER BY enrolled_at_quintile ASC, id) AS prev_running_count,
        score_dense_rank - LAG(score_dense_rank) OVER (ORDER BY running_count DESC, id) AS score_dense_rank_change
    FROM ranked
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        grade_status_group,
        second_enrolled_at_quintile,
        ROUND(STDDEV(last_score_percent_rank), 2) AS stddev_last_score_percent_rank,
        ROUND(SUM(grade_status_group_term_sum_score), 2) AS total_grade_status_group_term_sum_score,
        SUM(second_enrolled_at_quintile) AS total_second_enrolled_at_quintile,
        PERCENTILE_APPROX(second_enrolled_at_quintile, 0.5) AS median_second_enrolled_at_quintile,
        ROUND(STDDEV(last_score), 2) AS stddev_last_score
    FROM sequenced
    GROUP BY student_id, grade_status_group, second_enrolled_at_quintile
)
-- Final projection
SELECT
    student_id,
    grade_status_group,
    second_enrolled_at_quintile,
    total_second_enrolled_at_quintile,
    stddev_last_score,
    INITCAP(TRIM(grade_status_group)) AS grade_status_group_clean,
    LOWER(REGEXP_REPLACE(TRIM(grade_status_group), ' ', '_')) AS grade_status_group_slug
FROM grouped
ORDER BY grade_status_group_slug
