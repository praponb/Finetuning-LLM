WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE term IS NOT NULL
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status = 'FAIL'
              AND e.student_id >= 12
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        score,
        grade_status_ref,
        CASE WHEN term IN ('2025T1', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM qualified
    WHERE score > 32.9
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.gpa AS student_gpa
    FROM labeled s
    JOIN students st ON st.id = s.student_id
),
annotated AS (
    -- Label and score rows
    SELECT *, GREATEST(score, student_gpa) AS greatest_score_student_gpa
    FROM enriched_student
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_id, term_group, greatest_score_student_gpa
    FROM annotated
) src
PIVOT (
    MIN(greatest_score_student_gpa)
    FOR term_group IN ('other', 'group_a')
)
ORDER BY student_id
