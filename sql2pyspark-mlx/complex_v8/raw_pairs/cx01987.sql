WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('FAIL', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE grade_status = 'FAIL'
      AND course_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS prev_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        NTILE(5) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_quintile,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_2,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties_2, 3) AS enrolled_at_cume_dist_2,
        LAG(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS prev2_enrolled_at,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS seq_no,
        RANK() OVER (ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank
    FROM prepared_enrollments
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at NULLS LAST), w_all_by_enrolled_at_ties_2 AS (ORDER BY enrolled_at DESC NULLS FIRST), w_student_id_course_id_by_enro AS (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        grade_status_group,
        enrolled_at_cume_dist,
        prev_enrolled_at,
        last_enrolled_at,
        enrolled_at_quintile,
        minutes_since_prev_enrolled_at,
        first_enrolled_at,
        first_enrolled_at_2,
        enrolled_at_cume_dist_2,
        prev2_enrolled_at,
        enrolled_at_rank,
        enrolled_at_percent_rank
    FROM ranked
    WHERE seq_no = 1
      AND HOUR(prev_enrolled_at) BETWEEN 9 AND 13
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_prev2_enrolled_at AS prev2_enrolled_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrolled_at_quintile, DATE_TRUNC('MONTH', first_enrolled_at_2) ORDER BY enrolled_at_percent_rank DESC, id), 3) AS enrolled_at_percent_rank_percent_rank,
        RANK() OVER w_grade_status_group_last_enro AS first_enrolled_at_2_rank
    FROM leaders
    WINDOW w_all_by_prev2_enrolled_at AS (ORDER BY prev2_enrolled_at ASC, id), w_grade_status_group_last_enro AS (PARTITION BY grade_status_group, DATE_TRUNC('MONTH', last_enrolled_at) ORDER BY first_enrolled_at_2 DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_group,
    enrolled_at_cume_dist,
    prev_enrolled_at,
    last_enrolled_at,
    enrolled_at_quintile,
    minutes_since_prev_enrolled_at,
    first_enrolled_at,
    first_enrolled_at_2,
    enrolled_at_rank,
    enrolled_at_percent_rank,
    enrolled_at_percent_rank_percent_rank,
    first_enrolled_at_2_rank,
    COALESCE(enrolled_at_percent_rank, 0.0) AS enrolled_at_percent_rank_filled,
    TO_DATE(enrolled_at) AS enrolled_at_date
FROM ranked_2
ORDER BY enrolled_at_cume_dist
