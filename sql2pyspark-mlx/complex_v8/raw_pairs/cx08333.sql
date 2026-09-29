WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score < 83.1
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.student_name,
        st.major AS student_major,
        c.credits AS course_credits
    FROM filtered s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(course_credits) OVER (PARTITION BY student_id, student_major ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        ROUND(AVG(course_credits) OVER w_student_id, 2) AS student_id_avg_course_credits
    FROM enriched_student
    WINDOW w_student_id AS (PARTITION BY student_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, first_course_credits, course_credits
    FROM metrics
) src
UNPIVOT (
    metric_value FOR metric_name IN (first_course_credits, course_credits)
) unp
