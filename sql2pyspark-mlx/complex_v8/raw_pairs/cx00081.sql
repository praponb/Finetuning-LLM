WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND course_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(enrolled_at_month) ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg,
        SUM(score) OVER (ORDER BY enrolled_at_month DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        COUNT(*) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER w_enrolled_at_month_month_by_e, 3) AS enrolled_at_cume_dist
    FROM prepared
    WINDOW w_enrolled_at_month_month_by_e AS (PARTITION BY MONTH(enrolled_at_month) ORDER BY enrolled_at DESC)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_month,
        enrolled_at_rank,
        enrolled_at_dense_rank,
        score_share_of_course_id,
        score_vs_course_id_avg,
        score_sum_6_prior,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        LENGTH(TRIM(term)) AS term_len
    FROM metrics
    WHERE course_id IS NOT NULL
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(enrolled_at_month) AS enrolled_at_month_year,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) AS row_count,
        SUM(score_sum_6_prior) AS total_score_sum_6_prior,
        ROUND(STDDEV(score_vs_course_id_avg), 2) AS stddev_score_vs_course_id_avg
    FROM derived
    GROUP BY YEAR(enrolled_at_month)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY enrolled_at_month_year, row_count ORDER BY stddev_score_vs_course_id_avg, enrolled_at_month_year) AS stddev_score_vs_course_id_avg_quartile,
        LAG(total_score_sum_6_prior) OVER w_all_by_stddev_score_vs_cours AS prev_total_score_sum_6_prior,
        MIN(row_count) OVER (PARTITION BY row_count) AS row_count_min_row_count
    FROM grouped
    WINDOW w_all_by_stddev_score_vs_cours AS (ORDER BY stddev_score_vs_course_id_avg, enrolled_at_month_year)
)
-- Final projection
SELECT
    enrolled_at_month_year,
    term_list,
    row_count,
    total_score_sum_6_prior,
    stddev_score_vs_course_id_avg,
    stddev_score_vs_course_id_avg_quartile
FROM with_windows
ORDER BY total_score_sum_6_prior DESC, row_count, enrolled_at_month_year
LIMIT 10
