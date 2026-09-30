WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
      AND score >= 5.9
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(term)) AS term_clean,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM filtered
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.term = '2025T2'
),
unique_rows AS (
    -- Keep the latest row per student_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_clean,
        enrolled_at_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY enrolled_at_quarter DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term_clean,
    enrolled_at_quarter
FROM unique_rows
WHERE LENGTH(TRIM(term_clean)) >= 4
ORDER BY id NULLS LAST, term_clean
