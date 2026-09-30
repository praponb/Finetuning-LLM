WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IN ('2024T2', '2024T1', '2025T1')
),
annotated AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(term)) AS term_len
    FROM valid
    WHERE course_id >= 11
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        LENGTH(TRIM(term)) AS term_len_2
    FROM annotated
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, term_len_2, term_len
    FROM derived
) src
UNPIVOT (
    value FOR metric_name IN (term_len_2, term_len)
) unp
