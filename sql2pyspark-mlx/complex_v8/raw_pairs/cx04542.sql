WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CASE WHEN score >= 71.0 THEN 'HIGH' WHEN score >= 38.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term <> '2025T1'
      AND student_id <= 2
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
derived AS (
    -- Derive helper columns
    SELECT *, score + grade_status_len AS score_plus_grade_status_len
    FROM filtered
    WHERE student_id >= 4
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, score, score_plus_grade_status_len
    FROM derived
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (score, score_plus_grade_status_len)
) unp
