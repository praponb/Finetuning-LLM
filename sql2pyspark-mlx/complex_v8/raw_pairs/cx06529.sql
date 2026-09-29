WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-24 10:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        score - LAG(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at_plus_1d, id) AS score_change,
        SIZE(COLLECT_SET(course_id) OVER w_term) AS distinct_course_id_in_term,
        ROW_NUMBER() OVER (ORDER BY enrolled_at_plus_1d NULLS LAST, id) AS rn,
        LEAD(enrolled_at) OVER w_course_id_term_by_enrolled_a AS next_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, term ORDER BY enrolled_at_plus_1d DESC, id), 3) AS enrolled_at_plus_1d_cume_dist,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        DENSE_RANK() OVER w_course_id_grade_status_by_en AS enrolled_at_plus_1d_dense_rank,
        NTILE(10) OVER (ORDER BY enrolled_at_plus_1d NULLS LAST, id) AS enrolled_at_plus_1d_decile,
        SUM(score) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY enrolled_at_plus_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at
    FROM scoped
    WINDOW w_term AS (PARTITION BY term), w_course_id_term_by_enrolled_a AS (PARTITION BY course_id, term ORDER BY enrolled_at_plus_1d DESC, id), w_course_id_grade_status_by_en AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at_plus_1d DESC)
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_plus_1d,
    score_change,
    distinct_course_id_in_term,
    rn,
    next_enrolled_at,
    enrolled_at_plus_1d_cume_dist,
    rows_in_term,
    score_sum_1_prior,
    last_known_enrolled_at,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown
FROM sequenced
ORDER BY id
