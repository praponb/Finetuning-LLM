WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
),
with_details_student AS (
    -- Join students
    SELECT
        s.*,
        st.is_international AS student_is_international,
        c.dept AS course_dept,
        c.level AS course_level
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, score_adj, score
    FROM with_details_student
) src
UNPIVOT (
    amount FOR metric_name IN (score_adj AS m_score_adj, score AS m_score)
) unp
ORDER BY id, metric_name
