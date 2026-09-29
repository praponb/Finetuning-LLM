WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 11:00:00'
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status_term_label,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM recent_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id AND e.term <> '2025T1'
),
enriched AS (
    -- Attach enrollment attributes
    SELECT
        s.*,
        e.grade_status AS enrollment_grade_status_2,
        e.term AS enrollment_term_2
    FROM detail_rows_enrollments s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.term = '2024T1'
    WHERE e.term <> '2025T1'
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    COALESCE(term, 'ALL') AS term,
    GROUPING(student_id) AS is_total_student_id,
    SUM(score) FILTER (WHERE enrollment_term_2 = '2025T1') AS score_enrollment_term_2_2025t1,
    ROUND(100 * COUNT(CASE WHEN enrollment_term_2 = '2025T1' THEN 1 END) / COUNT(*), 1) AS pct_enrollment_term_2_2025t1
FROM qualified
GROUP BY GROUPING SETS ((student_id, term), (student_id))
ORDER BY student_id NULLS LAST, term NULLS LAST
