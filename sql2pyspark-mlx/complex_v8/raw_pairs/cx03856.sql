WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
),
enriched AS (
    -- Attach course attributes
    SELECT s.*, c.credits AS course_credits, c.dept AS course_dept
    FROM valid s
    LEFT JOIN courses c ON c.id = s.course_id
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score > 43.5
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_course_dept_term_by_course_c, 3) AS course_credits_percent_rank,
        NTILE(5) OVER (PARTITION BY course_dept, term ORDER BY course_credits, id) AS course_credits_quintile,
        NTILE(10) OVER w_course_dept_term_by_course_c AS course_credits_decile,
        MAX(course_credits) OVER (PARTITION BY course_dept, term ORDER BY course_credits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS course_credits_max_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_dept, term ORDER BY course_credits), 3) AS course_credits_percent_rank_2
    FROM screened
    WINDOW w_course_dept_term_by_course_c AS (PARTITION BY course_dept, term ORDER BY course_credits, id)
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at
    FROM sequenced s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_prefix,
        course_credits,
        course_dept,
        course_credits_decile,
        course_credits_max_6_prior,
        course_credits_percent_rank_2,
        first_enrollment_enrolled_at,
        ROUND(AVG(course_credits_percent_rank) OVER (PARTITION BY course_credits_quintile, student_id ORDER BY course_credits ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS course_credits_percent_rank_avg_1_prior,
        SUM(course_credits_max_6_prior) OVER (PARTITION BY course_credits_quintile, student_id ORDER BY course_credits ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS course_credits_max_6_prior_sum_6_prior,
        COUNT(*) OVER (PARTITION BY course_credits_max_6_prior, student_id) AS rows_in_course_credits_max_6_prior_student_id,
        ROUND(course_credits_percent_rank - AVG(course_credits_percent_rank) OVER w_course_credits_max_6_prior_s, 2) AS course_credits_percent_rank_vs_course_credits_ma,
        ROUND(CUME_DIST() OVER (ORDER BY course_credits_decile, id), 3) AS course_credits_decile_cume_dist
    FROM summarized_rows
    WINDOW w_course_credits_max_6_prior_s AS (PARTITION BY course_credits_max_6_prior, student_id)
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    course_dept,
    course_credits_decile,
    course_credits_max_6_prior,
    first_enrollment_enrolled_at,
    course_credits_max_6_prior_sum_6_prior,
    rows_in_course_credits_max_6_prior_student_id,
    course_credits_percent_rank_vs_course_credits_ma,
    course_credits_decile_cume_dist,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
FROM windowed
