WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CASE WHEN score >= 57.0 THEN 'HIGH' WHEN score >= 30.0 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
),
qualified AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND UPPER(TRIM(c.course_code)) LIKE 'C%'
        )
)
SELECT
    COALESCE(term, 'ALL') AS term,
    student_id,
    MAX(enrolled_at) AS last_enrolled_at
FROM qualified
GROUP BY GROUPING SETS ((term, student_id), (term), (student_id), ())
