WITH pass_by_course AS (
    -- PASS rows per course_id
    SELECT course_id, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY course_id
),
withdrawn_by_course AS (
    -- WITHDRAWN rows per course_id
    SELECT course_id, COUNT(*) AS withdrawn_count, ROUND(AVG(score), 2) AS withdrawn_avg_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY course_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.course_id, b.course_id) AS course_id,
    COALESCE(a.pass_count, 0) AS pass_count,
    a.pass_avg_score,
    COALESCE(b.withdrawn_count, 0) AS withdrawn_count,
    b.withdrawn_avg_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.withdrawn_count, 0) AS pass_count_minus_withdrawn_count
FROM pass_by_course a
FULL OUTER JOIN withdrawn_by_course b
  ON a.course_id = b.course_id
ORDER BY course_id
