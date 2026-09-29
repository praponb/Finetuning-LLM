WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE score < 59.8
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY course_id, term_or_unknown ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        RANK() OVER w_term_or_unknown_course_id_by_2 AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY term_or_unknown, student_id ORDER BY enrolled_at DESC, id) AS enrolled_at_seq,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term_or_unknown, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, term_or_unknown ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        LEAD(enrolled_at) OVER (PARTITION BY course_id, term_or_unknown ORDER BY enrolled_at DESC, id) AS next_enrolled_at,
        ROW_NUMBER() OVER w_term_or_unknown_course_id_by_3 AS seq_no,
        DENSE_RANK() OVER w_term_or_unknown_course_id_by_2 AS enrolled_at_dense_rank_4
    FROM valid
    WINDOW w_term_or_unknown_course_id_by_2 AS (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at), w_course_id_term_or_unknown_by AS (PARTITION BY course_id, term_or_unknown ORDER BY enrolled_at DESC), w_term_or_unknown_course_id_by_3 AS (PARTITION BY term_or_unknown, course_id ORDER BY enrolled_at, id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    second_enrolled_at,
    enrolled_at_dense_rank,
    enrolled_at_rank,
    enrolled_at_seq,
    minutes_since_prev_enrolled_at,
    enrolled_at_percent_rank,
    last_enrolled_at,
    enrolled_at_cume_dist,
    next_enrolled_at,
    enrolled_at_dense_rank_4,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
    ROUND((UNIX_TIMESTAMP(next_enrolled_at) - UNIX_TIMESTAMP(last_enrolled_at)) / 3600, 1) AS hours_last_enrolled_at_to_next_enrolled_at
FROM metrics
