WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND grade_status NOT IN ('PASS', 'FAIL')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        MAX(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        LEAD(enrolled_at) OVER w_all_by_score AS next_enrolled_at,
        SUM(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        MIN(score) OVER (PARTITION BY term) AS term_min_score,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg,
        COUNT(*) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM valid_enrollments
    WINDOW w_all_by_score AS (ORDER BY score DESC, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(term_min_score) OVER (PARTITION BY YEAR(next_enrolled_at) ORDER BY rows_in_course_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_term_min_score,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(next_enrolled_at))) AS distinct_course_id_in_next_enrolled_at_month,
        ROUND(AVG(course_id_avg_score) OVER (PARTITION BY MONTH(next_enrolled_at)), 2) AS next_enrolled_at_month_avg_course_id_avg_score,
        ROUND(AVG(score_sum_2_prior) OVER (PARTITION BY student_id ORDER BY course_id_avg_score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_sum_2_prior_avg_3,
        NTILE(10) OVER (PARTITION BY student_id ORDER BY course_id_avg_score, id) AS course_id_avg_score_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(next_enrolled_at) ORDER BY rows_in_course_id DESC NULLS FIRST), 3) AS rows_in_course_id_percent_rank
    FROM sequenced
),
rollup_base AS (
    -- Group-level totals
    SELECT
        distinct_course_id_in_next_enrolled_at_month,
        course_id,
        grade_status_or_unknown,
        ROUND(SUM(last_term_min_score) / NULLIF(SUM(course_id_avg_score), 0), 3) AS last_term_min_score_to_course_id_avg_score_ratio,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(last_term_min_score), 2) AS stddev_last_term_min_score
    FROM windowed
    WHERE grade_status = 'PASS'
    GROUP BY distinct_course_id_in_next_enrolled_at_month, course_id, grade_status_or_unknown
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER w_distinct_ids) AS distinct_course_id_in_distinct_ids,
        FIRST_VALUE(stddev_last_term_min_score) OVER (ORDER BY last_term_min_score_to_course_id_avg_score_ratio DESC, distinct_course_id_in_next_enrolled_at_month, course_id, grade_status_or_unknown ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_stddev_last_term_min_score
    FROM rollup_base
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    distinct_course_id_in_next_enrolled_at_month,
    course_id,
    grade_status_or_unknown,
    last_term_min_score_to_course_id_avg_score_ratio,
    stddev_last_term_min_score,
    distinct_course_id_in_distinct_ids,
    first_stddev_last_term_min_score
FROM with_windows
ORDER BY last_term_min_score_to_course_id_avg_score_ratio, distinct_course_id_in_distinct_ids DESC
