WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE course_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per term, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_ref,
        score_filled,
        term_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, course_id ORDER BY term_len DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        term_ref,
        score_filled,
        term_len,
        ROUND(AVG(term_len) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS term_len_avg_6_prior,
        FIRST_VALUE(term_len) IGNORE NULLS OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_term_len,
        LAG(term_len, 1, 0) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id) AS prev_term_len
    FROM latest
)
SELECT
    course_id,
    term_len,
    student_id,
    GROUPING(course_id) AS is_total_course_id,
    ROUND(AVG(term_len), 2) AS avg_term_len
FROM sequenced
GROUP BY GROUPING SETS ((course_id, term_len, student_id), (course_id))
ORDER BY course_id NULLS LAST, term_len NULLS LAST, student_id NULLS LAST
