WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        COALESCE(score, 0.0) AS score_filled_2,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
      AND term <> '2025T1'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM prepared_enrollments
    WHERE score_filled >= 20.0
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term = '2025T1'
        )
)
-- Subtotals with ROLLUP
SELECT
    grade_status,
    term,
    GROUPING_ID() AS gid,
    MAX(enrolled_at) AS last_enrolled_at
FROM qualified
GROUP BY ROLLUP(grade_status, term)
