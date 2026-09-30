WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE course_id <= 4
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        c.course_code,
        c.level AS course_level,
        c.credits AS course_credits
    FROM cleaned s
    JOIN courses c ON c.id = s.course_id
    WHERE c.course_code IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        course_code,
        course_level,
        course_credits,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY MONTH(enrolled_at)), 0), 4) AS course_credits_share_of_enrolled_at_month,
        RANK() OVER (PARTITION BY student_id, course_level ORDER BY score DESC) AS score_rank,
        MIN(score) OVER (PARTITION BY MONTH(enrolled_at)) AS enrolled_at_month_min_score,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at)) AS rows_in_enrolled_at_month,
        FIRST_VALUE(course_credits) OVER (PARTITION BY student_id, course_level ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(enrolled_at))) AS distinct_course_id_in_enrolled_at_month,
        SUM(course_credits) OVER (PARTITION BY course_credits, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits,
        LAG(course_credits, 2, 0) OVER (PARTITION BY student_id, YEAR(enrolled_at) ORDER BY course_credits ASC, id) AS prev2_course_credits,
        LEAD(enrolled_at) OVER (PARTITION BY student_id, YEAR(enrolled_at) ORDER BY course_credits ASC, id) AS next_enrolled_at,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS course_credits_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, course_level ORDER BY score DESC), 3) AS score_percent_rank,
        MAX(score) OVER (PARTITION BY course_credits, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_credits, YEAR(enrolled_at) ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id, course_level ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        COUNT(*) OVER (PARTITION BY course_credits, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (PARTITION BY student_id, course_level ORDER BY score DESC, id) AS score_dense_rank
    FROM with_details
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    course_code,
    course_level,
    course_credits,
    enrolled_at_month_min_score,
    rows_in_enrolled_at_month,
    first_course_credits,
    distinct_course_id_in_enrolled_at_month,
    running_course_credits,
    next_enrolled_at,
    course_credits_avg_4,
    score_percent_rank,
    score_max_7,
    enrolled_at_percent_rank,
    running_count,
    score_dense_rank,
    MONTH(enrolled_at) AS enrolled_at_month
FROM sequenced
ORDER BY course_credits_avg_4 DESC, id
LIMIT 10
