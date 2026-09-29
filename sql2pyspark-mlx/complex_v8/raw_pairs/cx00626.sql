WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND term = '2024T1'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term NOT IN ('2024T2', '2025T2')
              AND e.course_id IS NOT NULL
        )
),
annotated AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM qualified
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
)
SELECT
    id,
    student_id,
    grade_status,
    grade_status_clean,
    DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
FROM annotated
WHERE student_id >= 13
ORDER BY id DESC
