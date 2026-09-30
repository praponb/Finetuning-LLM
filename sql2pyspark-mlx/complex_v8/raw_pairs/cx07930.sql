WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'WITHDRAWN')
      AND student_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_course_id_term_by_enrolled_a, 3) AS enrolled_at_cume_dist,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, YEAR(enrolled_at) ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist_2,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY TO_DATE(enrolled_at), grade_status ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, YEAR(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_course_id_term_by_enrolled_a_2)) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM base
    WINDOW w_course_id_term_by_enrolled_a AS (PARTITION BY course_id, term ORDER BY enrolled_at), w_course_id_term_by_enrolled_a_2 AS (PARTITION BY course_id, term ORDER BY enrolled_at, id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    term_ref,
    last_known_enrolled_at,
    enrolled_at_cume_dist_2,
    enrolled_at_rank,
    second_enrolled_at,
    first_enrolled_at,
    last_known_enrolled_at_2,
    minutes_since_prev_enrolled_at,
    YEAR(second_enrolled_at_2) AS second_enrolled_at_2_year,
    FLOOR(enrolled_at_rank / 2) * 2 AS enrolled_at_rank_bucket
FROM metrics
ORDER BY enrolled_at_rank DESC, last_known_enrolled_at_2 DESC, id
LIMIT 20
