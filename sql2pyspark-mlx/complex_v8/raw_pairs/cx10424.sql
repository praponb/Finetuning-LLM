WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN dept IN ('Math', 'Biz') THEN 'group_a' WHEN dept = 'Bio' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE level = 'intro'
      AND LOWER(course_code) LIKE '%10%'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) FILTER (WHERE term = '2024T1') AS enrollment_term_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM scoped_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        dept_group,
        last_enrollment_enrolled_at,
        credits - LAG(credits) OVER w_level_dept_by_enrollment_ter AS credits_change,
        NTH_VALUE(credits, 2) OVER (PARTITION BY level, dept_group ORDER BY enrollment_term_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        ROUND(credits - AVG(credits) OVER (PARTITION BY level), 2) AS credits_vs_level_avg,
        RANK() OVER w_level_dept_by_enrollment_ter AS enrollment_term_count_rank,
        LAST_VALUE(credits) OVER (PARTITION BY level, dept ORDER BY enrollment_term_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_credits,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level
    FROM summarized_rows
    WINDOW w_level_dept_by_enrollment_ter AS (PARTITION BY level, dept ORDER BY enrollment_term_count, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        dept,
        SUM(CASE WHEN LOWER(course_code) LIKE '%00%' THEN 1 ELSE 0 END) AS n_lower_course_code_like_00,
        MAX(rows_in_level) AS max_rows_in_level
    FROM windowed
    WHERE enrollment_term_count_rank BETWEEN 3 AND 9
    GROUP BY dept
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE max_rows_in_level > (SELECT AVG(max_rows_in_level) FROM rollup_base)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        dept,
        ROUND(max_rows_in_level / NULLIF(SUM(max_rows_in_level) OVER (PARTITION BY n_lower_course_code_like_00), 0), 4) AS max_rows_in_level_share_of_n_lower_course_code_l,
        DENSE_RANK() OVER w_n_lower_course_code_like_00_2 AS n_lower_course_code_like_00_dense_rank,
        NTH_VALUE(max_rows_in_level, 2) OVER (ORDER BY n_lower_course_code_like_00 ASC, dept ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_rows_in_level,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY n_lower_course_code_like_00)) AS distinct_dept_in_n_lower_course_code_like_00,
        SUM(n_lower_course_code_like_00) OVER (PARTITION BY n_lower_course_code_like_00) AS n_lower_course_code_like_00_sum_n_lower_course_c
    FROM screened
    WINDOW w_n_lower_course_code_like_00_2 AS (PARTITION BY n_lower_course_code_like_00, dept ORDER BY n_lower_course_code_like_00, dept), w_n_lower_course_code_like_00 AS (PARTITION BY n_lower_course_code_like_00)
)
-- Final projection
SELECT
    dept,
    max_rows_in_level_share_of_n_lower_course_code_l,
    n_lower_course_code_like_00_dense_rank,
    second_max_rows_in_level,
    distinct_dept_in_n_lower_course_code_like_00,
    n_lower_course_code_like_00_sum_n_lower_course_c,
    CASE WHEN dept IN ('Math', 'Bio') THEN 'group_a' WHEN dept = 'Biz' THEN 'group_b' ELSE 'other' END AS dept_group,
    FLOOR(second_max_rows_in_level / 5) * 5 AS second_max_rows_in_level_bucket
FROM metrics
ORDER BY dept DESC, dept_group
