WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE term IN ('2025T2', '2024T1', '2024T2')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC, id) AS enrolled_at_quintile,
        NTILE(4) OVER w_all_by_enrolled_at AS enrolled_at_quartile
    FROM base_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, enrolled_at_quintile, enrolled_at_quartile
    FROM windowed
) src
UNPIVOT (
    amount FOR field IN (enrolled_at_quintile, enrolled_at_quartile)
) unp
ORDER BY id, field
