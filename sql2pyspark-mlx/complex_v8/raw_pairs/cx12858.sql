WITH base AS (
    -- Scope the enrollments rows
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
    WHERE course_id <= 9
      AND score >= 32.7
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        c.dept AS course_dept,
        c.level AS course_level
    FROM base s
    JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_level, course_dept ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_level, course_dept ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        LAST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_level, course_dept ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY term, course_level ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY term, course_level ORDER BY enrolled_at NULLS LAST, id), 3) AS enrolled_at_cume_dist_2,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS rn,
        DENSE_RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY term, course_level ORDER BY enrolled_at NULLS LAST, id) AS rn_2
    FROM joined
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at ASC)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        course_dept,
        course_level,
        second_enrolled_at,
        enrolled_at_percent_rank,
        last_enrolled_at,
        last_known_enrolled_at,
        enrolled_at_cume_dist,
        last_enrolled_at_2,
        minutes_since_prev_enrolled_at,
        enrolled_at_cume_dist_2,
        rn,
        enrolled_at_dense_rank
    FROM sequenced
    WHERE rn_2 <= 2
      AND course_id >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        course_dept,
        course_level,
        last_enrolled_at,
        enrolled_at_cume_dist,
        last_enrolled_at_2,
        minutes_since_prev_enrolled_at,
        enrolled_at_cume_dist_2,
        enrolled_at_dense_rank,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        ROUND(enrolled_at_cume_dist_2 - AVG(enrolled_at_cume_dist_2) OVER (PARTITION BY course_level), 2) AS enrolled_at_cume_dist_2_vs_course_level_avg,
        ROUND(AVG(enrolled_at_dense_rank) OVER (ORDER BY second_enrolled_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS enrolled_at_dense_rank_avg_2,
        MAX(enrolled_at_cume_dist) OVER (ORDER BY second_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_enrolled_at_cume_dist_so_far
    FROM first_rows
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    course_level,
    last_enrolled_at,
    enrolled_at_cume_dist,
    last_enrolled_at_2,
    minutes_since_prev_enrolled_at,
    enrolled_at_cume_dist_2,
    enrolled_at_dense_rank,
    rows_in_course_level,
    enrolled_at_cume_dist_2_vs_course_level_avg,
    max_enrolled_at_cume_dist_so_far
FROM windowed
ORDER BY last_enrolled_at
