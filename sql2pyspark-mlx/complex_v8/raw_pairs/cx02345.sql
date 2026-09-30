WITH prepared AS (
    -- Filter students
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international,
        CASE WHEN major IN ('CS', 'Math') THEN 'group_a' WHEN major = 'Bio' THEN 'group_b' ELSE 'other' END AS major_group
    FROM students
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        enrolled_on,
        is_international,
        INITCAP(TRIM(student_name)) AS student_name_clean,
        DAYOFWEEK(enrolled_on) AS enrolled_on_dayofweek
    FROM prepared
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(CASE WHEN term <> '2025T1' THEN 1 END) AS enrollment_term_count,
        COUNT(*) AS enrollment_count,
        COUNT(*) FILTER (WHERE term IN ('2024T1', '2024T2', '2025T2')) AS enrollment_term_count_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_term_count_2, 0) AS enrollment_term_count_2
    FROM labeled s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    enrolled_on_dayofweek,
    enrollment_count,
    MAX(enrollment_count) AS max_enrollment_count
FROM with_counts
GROUP BY ROLLUP(enrolled_on_dayofweek, enrollment_count)
ORDER BY enrolled_on_dayofweek NULLS LAST, enrollment_count NULLS LAST
