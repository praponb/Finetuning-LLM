WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE (grade_status NOT IN ('FAIL', 'WITHDRAWN') OR term NOT IN ('2025T2', '2024T1'))
),
deduped AS (
    -- Keep the earliest row per term, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, grade_status ORDER BY enrolled_at, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        term,
        course_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM deduped
    WHERE course_id <= 11
    GROUP BY term, course_id
)
-- Final projection
SELECT
    term,
    course_id,
    row_count,
    distinct_ids,
    distinct_grade_status,
    CASE WHEN term IN ('2025T1', '2025T2') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
    ABS(distinct_grade_status - 5) AS distinct_grade_status_distance
FROM rollup_base
ORDER BY term DESC, course_id
LIMIT 20
