WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score >= 36.6
      AND HOUR(enrolled_at) BETWEEN 8 AND 14
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.level AS course_level,
        c.course_code
    FROM recent s
    JOIN courses c ON c.id = s.course_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_level, grade_status, score
    FROM with_parent
) src
PIVOT (
    AVG(score)
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'FAIL')
)
