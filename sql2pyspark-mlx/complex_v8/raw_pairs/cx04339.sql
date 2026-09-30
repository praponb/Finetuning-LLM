WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE term IN ('2025T2', '2024T2')
),
unique_rows AS (
    -- Keep the latest row per course_id, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_ref,
        enrolled_at_plus_30m,
        grade_status_term_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at_plus_30m DESC, id DESC) AS rn
        FROM scoped_enrollments
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY student_id, course_id ORDER BY score) AS score_dense_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        FIRST_VALUE(enrolled_at_plus_30m) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_plus_30m,
        LEAD(enrolled_at_plus_30m) OVER (ORDER BY score ASC, id) AS next_enrolled_at_plus_30m,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, course_id), 2) AS score_vs_student_id_course_id_avg
    FROM unique_rows
)
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    term_ref,
    enrolled_at_plus_30m,
    grade_status_term_label,
    score_dense_rank,
    first_known_score,
    first_known_enrolled_at_plus_30m,
    next_enrolled_at_plus_30m,
    score_vs_student_id_course_id_avg,
    FLOOR(score / 25) * 25 AS score_bucket,
    QUARTER(enrolled_at) AS enrolled_at_quarter
FROM with_windows
ORDER BY course_id DESC, enrolled_at
