WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
      AND score BETWEEN 16.3 AND 85.2
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        st.major AS student_major
    FROM recent_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = FALSE
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        student_cohort,
        student_is_international,
        NTILE(4) OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id) AS enrolled_at_quartile,
        DENSE_RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_dense_rank,
        RANK() OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC) AS enrolled_at_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAG(enrolled_at, 2) OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id) AS prev2_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAG(enrolled_at, 2) OVER w_all_by_enrolled_at_2 AS prev2_enrolled_at_2,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        LAG(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC, id) AS prev2_enrolled_at_3,
        ROUND(PERCENT_RANK() OVER w_all_by_enrolled_at_ties_2, 3) AS enrolled_at_percent_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id), 3) AS enrolled_at_cume_dist
    FROM enriched_student
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at DESC), w_all_by_enrolled_at_2 AS (ORDER BY enrolled_at, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_all_by_enrolled_at_ties_2 AS (ORDER BY enrolled_at)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    student_is_international,
    enrolled_at_dense_rank,
    enrolled_at_rank,
    last_known_enrolled_at,
    minutes_since_prev_enrolled_at,
    first_enrolled_at,
    prev2_enrolled_at_2,
    rn,
    enrolled_at_percent_rank,
    prev2_enrolled_at_3,
    enrolled_at_percent_rank_2,
    rn + enrolled_at_percent_rank AS rn_plus_enrolled_at_percent_rank,
    DATEDIFF(DATE '2025-06-30', prev2_enrolled_at_3) AS days_since_prev2_enrolled_at_3
FROM windowed
ORDER BY id DESC NULLS FIRST, last_known_enrolled_at DESC NULLS FIRST
LIMIT 20
