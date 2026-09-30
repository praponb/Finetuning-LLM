WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
),
qualified AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM filtered_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.course_code IS NOT NULL
        )
),
latest AS (
    -- Keep the latest row per course_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, term ORDER BY score DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        term,
        LENGTH(TRIM(term)) AS term_len,
        ABS(score - 59.7) AS score_distance,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM latest
)
SELECT
    id,
    term,
    term_len,
    score_distance,
    enrolled_at_quarter,
    term_len - enrolled_at_quarter AS term_len_minus_enrolled_at_quarter,
    ROUND(score_distance * 1.5, 2) AS score_distance_adj
FROM annotated
ORDER BY score_distance DESC, id
LIMIT 5
