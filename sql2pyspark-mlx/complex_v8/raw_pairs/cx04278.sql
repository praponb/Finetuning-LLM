WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, term ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        LEAD(enrolled_at, 2) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id) AS next2_enrolled_at,
        NTILE(5) OVER (PARTITION BY TO_DATE(enrolled_at), term ORDER BY enrolled_at DESC, id) AS enrolled_at_quintile,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at, id) AS prev_enrolled_at,
        ROW_NUMBER() OVER w_enrolled_at_to_date_term_by AS rn,
        NTILE(10) OVER (ORDER BY enrolled_at, id) AS enrolled_at_decile,
        RANK() OVER (PARTITION BY TO_DATE(enrolled_at), term ORDER BY enrolled_at DESC) AS enrolled_at_rank_2,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term_group, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LAG(enrolled_at) OVER w_enrolled_at_to_date_term_by AS prev_enrolled_at_2,
        NTILE(4) OVER (ORDER BY enrolled_at, id) AS enrolled_at_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist_2
    FROM prepared_enrollments
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_enrolled_at_to_date_term_by AS (PARTITION BY TO_DATE(enrolled_at), term ORDER BY enrolled_at DESC, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_group,
        enrolled_at_percent_rank,
        first_enrolled_at,
        enrolled_at_cume_dist,
        next2_enrolled_at,
        enrolled_at_quintile,
        minutes_since_prev_enrolled_at,
        prev_enrolled_at,
        rn,
        enrolled_at_decile,
        enrolled_at_rank_2,
        last_known_enrolled_at,
        prev_enrolled_at_2,
        enrolled_at_quartile,
        enrolled_at_cume_dist_2
    FROM metrics
    WHERE enrolled_at_rank <= 3
      AND enrolled_at_percent_rank < 0.8
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    term_group,
    first_enrolled_at,
    enrolled_at_cume_dist,
    enrolled_at_quintile,
    prev_enrolled_at,
    rn,
    enrolled_at_decile,
    enrolled_at_rank_2,
    last_known_enrolled_at,
    prev_enrolled_at_2,
    enrolled_at_quartile,
    enrolled_at_cume_dist_2
FROM first_rows
ORDER BY id DESC, course_id
