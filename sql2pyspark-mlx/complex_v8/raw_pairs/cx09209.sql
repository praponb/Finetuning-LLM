WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND term IS NOT NULL
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrolled_at_date,
        s.grade_status_clean,
        c.credits AS course_credits,
        c.course_code,
        c.level AS course_level
    FROM cleaned s
    JOIN courses c ON c.id = s.course_id
    WHERE c.credits BETWEEN 2 AND 4
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level, student_id ORDER BY course_credits NULLS LAST, id), 3) AS course_credits_cume_dist,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        NTILE(5) OVER (PARTITION BY course_level, student_id ORDER BY course_credits NULLS LAST, id) AS course_credits_quintile,
        SUM(score) OVER (PARTITION BY course_level) AS course_level_sum_score,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level, student_id ORDER BY course_credits NULLS LAST), 3) AS course_credits_cume_dist_2
    FROM with_parent
    WINDOW w_course_level AS (PARTITION BY course_level)
),
grouped AS (
    -- Group-level totals
    SELECT
        course_credits_quintile,
        course_level,
        ROUND(STDDEV(course_credits), 2) AS stddev_course_credits,
        COUNT(*) AS n_rows
    FROM windowed
    WHERE term = '2024T1'
    GROUP BY 1, 2
    HAVING SUM(course_credits_cume_dist) > 0.4
),
metrics AS (
    -- Previous / next values per key
    SELECT
        course_credits_quintile,
        course_level,
        stddev_course_credits,
        n_rows,
        COUNT(*) OVER w_course_credits_quintile_cour AS rows_in_course_credits_quintile_course_level,
        ROUND(AVG(n_rows) OVER (PARTITION BY course_credits_quintile ORDER BY stddev_course_credits DESC, course_credits_quintile, course_level ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS n_rows_avg_2,
        ROUND(stddev_course_credits - AVG(stddev_course_credits) OVER (PARTITION BY course_credits_quintile, course_level), 2) AS stddev_course_credits_vs_course_credits_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY course_credits_quintile ORDER BY stddev_course_credits DESC, course_credits_quintile, course_level), 3) AS stddev_course_credits_cume_dist,
        LAST_VALUE(course_credits_quintile) OVER (PARTITION BY course_credits_quintile ORDER BY stddev_course_credits DESC, course_credits_quintile, course_level ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level, course_credits_quintile ORDER BY n_rows), 3) AS n_rows_cume_dist
    FROM grouped
    WINDOW w_course_credits_quintile_cour AS (PARTITION BY course_credits_quintile, course_level)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        course_credits_quintile,
        course_level,
        stddev_course_credits,
        n_rows,
        rows_in_course_credits_quintile_course_level,
        n_rows_avg_2,
        stddev_course_credits_vs_course_credits_quintile,
        last_course_credits_quintile,
        SUM(n_rows_avg_2) OVER (ORDER BY last_course_credits_quintile ASC, course_credits_quintile, course_level ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_rows_avg_2,
        ROUND(last_course_credits_quintile - AVG(last_course_credits_quintile) OVER w_n_rows_course_level, 2) AS last_course_credits_quintile_vs_n_rows_course_le,
        LAG(last_course_credits_quintile, 1, 0) OVER w_n_rows_by_n_rows AS prev_last_course_credits_quintile,
        LEAD(n_rows_avg_2) OVER (PARTITION BY n_rows ORDER BY n_rows, course_credits_quintile, course_level) AS next_n_rows_avg_2,
        DENSE_RANK() OVER w_n_rows_by_n_rows_ties AS n_rows_dense_rank
    FROM metrics
    WINDOW w_n_rows_course_level AS (PARTITION BY n_rows, course_level), w_n_rows_by_n_rows AS (PARTITION BY n_rows ORDER BY n_rows, course_credits_quintile, course_level), w_n_rows_by_n_rows_ties AS (PARTITION BY n_rows ORDER BY n_rows)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE n_rows_dense_rank = 1
      AND prev_last_course_credits_quintile > 3
)
SELECT
    course_credits_quintile,
    course_level,
    stddev_course_credits,
    n_rows,
    rows_in_course_credits_quintile_course_level,
    last_course_credits_quintile,
    last_course_credits_quintile_vs_n_rows_course_le,
    prev_last_course_credits_quintile,
    next_n_rows_avg_2,
    n_rows_dense_rank,
    LOWER(TRIM(course_level)) AS course_level_clean,
    n_rows + n_rows_avg_2 AS n_rows_plus_n_rows_avg_2
FROM top_rows
ORDER BY last_course_credits_quintile_vs_n_rows_course_le DESC NULLS FIRST, rows_in_course_credits_quintile_course_level DESC
