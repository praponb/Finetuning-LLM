WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        ROUND(score * 1.5, 2) AS score_adj
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-24'
      AND score > 42.4
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        st.major AS student_major
    FROM recent s
    JOIN students st ON st.id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, grade_status, score
    FROM joined
) src
PIVOT (
    AVG(score)
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE')
)
