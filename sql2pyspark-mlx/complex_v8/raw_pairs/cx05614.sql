WITH valid_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE dept <> 'Biz'
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_courses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND HOUR(e.enrolled_at) BETWEEN 6 AND 11
        )
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        dept,
        ROUND(STDDEV(credits), 2) AS stddev_credits,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(course_code)) >= 7) AS n_length_trim_course_code_7,
        SUM(credits) AS total_credits,
        ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
        COUNT(*) FILTER (WHERE UPPER(TRIM(course_code)) LIKE 'A%') AS n_upper_trim_course_code_like
    FROM screened
    WHERE LOWER(course_code) LIKE '%11%'
    GROUP BY dept
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(n_length_trim_course_code_7) OVER w_all_by_total_credits AS prev_n_length_trim_course_code_7,
        COUNT(*) OVER (PARTITION BY n_upper_trim_course_code_like, dept) AS rows_in_n_upper_trim_course_code_like_dept,
        RANK() OVER w_all_by_total_credits_ties AS total_credits_rank
    FROM rollup_base
    WINDOW w_all_by_total_credits AS (ORDER BY total_credits DESC, dept), w_all_by_total_credits_ties AS (ORDER BY total_credits DESC)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY total_credits_rank) AS rows_in_total_credits_rank,
        SUM(total_credits) OVER (ORDER BY n_length_trim_course_code_7 DESC, dept ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_credits,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY total_credits_rank)) AS distinct_dept_in_total_credits_rank,
        ROUND(AVG(total_credits_rank) OVER (PARTITION BY total_credits_rank), 2) AS total_credits_rank_avg_total_credits_rank,
        NTILE(3) OVER (ORDER BY prev_n_length_trim_course_code_7 DESC, dept) AS prev_n_length_trim_course_code_7_tercile
    FROM ranked
    WINDOW w_total_credits_rank AS (PARTITION BY total_credits_rank)
)
-- Final projection
SELECT
    dept,
    stddev_credits,
    n_length_trim_course_code_7,
    total_credits,
    course_code_list,
    prev_n_length_trim_course_code_7,
    rows_in_n_upper_trim_course_code_like_dept,
    total_credits_rank,
    rows_in_total_credits_rank,
    running_total_credits,
    distinct_dept_in_total_credits_rank,
    total_credits_rank_avg_total_credits_rank,
    prev_n_length_trim_course_code_7_tercile,
    LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
FROM ranked_2
