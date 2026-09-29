WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 14:00:00'
),
with_parent_student AS (
    -- Join students
    SELECT s.*, st.gpa AS student_gpa, st.enrolled_on AS student_enrolled_on
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE YEAR(st.enrolled_on) = 2025
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        INITCAP(TRIM(term)) AS term_clean
    FROM with_parent_student
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, student_gpa, score
    FROM derived
) src
UNPIVOT (
    metric_value FOR measure IN (student_gpa AS student_gpa_v, score AS score_v)
) unp
