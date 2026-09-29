WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
    FROM enrollments
    WHERE (grade_status <> 'INCOMPLETE' OR term <> '2024T1')
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_plus_7d,
        c.credits AS course_credits,
        c.course_code,
        c.level AS course_level
    FROM prepared s
    JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        course_credits,
        course_level,
        LAG(enrolled_at_plus_7d) OVER (ORDER BY course_credits DESC, id) AS prev_enrolled_at_plus_7d,
        LAST_VALUE(course_credits) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits,
        LAG(enrolled_at_plus_7d, 3) OVER (ORDER BY course_credits DESC, id) AS prev3_enrolled_at_plus_7d,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY YEAR(enrolled_at_plus_7d))) AS distinct_term_in_enrolled_at_plus_7d_year,
        FIRST_VALUE(enrolled_at_plus_7d) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_plus_7d,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at_plus_7d)), 0), 4) AS score_share_of_enrolled_at_plus_7d_year,
        MAX(course_credits) OVER (PARTITION BY YEAR(enrolled_at_plus_7d)) AS enrolled_at_plus_7d_year_max_course_credits,
        LAG(score, 2) OVER (ORDER BY course_credits DESC, id) AS prev2_score,
        MIN(course_credits) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_course_credits_so_far,
        NTILE(3) OVER (PARTITION BY student_id, course_credits ORDER BY enrolled_at_plus_7d, id) AS enrolled_at_plus_7d_tercile,
        ROUND(AVG(course_credits) OVER w_enrolled_at_plus_7d_year, 2) AS enrolled_at_plus_7d_year_avg_course_credits,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at
    FROM combined_details_course
    WINDOW w_enrolled_at_plus_7d_year AS (PARTITION BY YEAR(enrolled_at_plus_7d))
),
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(course_credits, 0) AS course_credits_filled
    FROM ranked
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY course_credits, id) AS course_credits_seq,
        SUM(course_credits_filled) OVER (PARTITION BY YEAR(prev3_enrolled_at_plus_7d), grade_status) AS prev3_enrolled_at_plus_7d_year_grade_status_sum_,
        RANK() OVER (ORDER BY prev3_enrolled_at_plus_7d DESC) AS prev3_enrolled_at_plus_7d_rank
    FROM tagged
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    course_credits,
    last_course_credits,
    prev3_enrolled_at_plus_7d,
    distinct_term_in_enrolled_at_plus_7d_year,
    first_enrolled_at_plus_7d,
    score_share_of_enrolled_at_plus_7d_year,
    prev2_score,
    enrolled_at_plus_7d_tercile,
    enrolled_at_plus_7d_year_avg_course_credits,
    last_enrolled_at,
    course_credits_filled,
    course_credits_seq,
    prev3_enrolled_at_plus_7d_year_grade_status_sum_,
    prev3_enrolled_at_plus_7d_rank,
    LOWER(TRIM(grade_status)) AS grade_status_clean
FROM with_windows
ORDER BY student_id, course_credits_seq DESC, id
LIMIT 20
