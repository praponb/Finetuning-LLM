WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 80.2 THEN 'A' WHEN score >= 20.6 THEN 'B' ELSE 'C' END AS score_tier,
        ABS(score - 57.8) AS score_distance
    FROM enrollments
    WHERE course_id >= 2
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status NOT IN ('INCOMPLETE', 'FAIL')
        )
      AND s.student_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM screened s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.term NOT IN ('2024T2', '2025T2')
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, enrolled_at, grade_status, score
    FROM qualified
) src
PIVOT (
    MIN(score) AS min_v, COUNT(score) AS n
    FOR grade_status IN ('INCOMPLETE', 'PASS')
)
ORDER BY course_id, enrolled_at
