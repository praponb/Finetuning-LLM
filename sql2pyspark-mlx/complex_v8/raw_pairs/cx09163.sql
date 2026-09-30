WITH fail_by_course AS (
    -- FAIL rows per course_id
    SELECT course_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY course_id
),
pass_by_course AS (
    -- PASS rows per course_id
    SELECT course_id, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY course_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.course_id, b.course_id) AS course_id,
    COALESCE(a.fail_count, 0) AS fail_count,
    a.fail_total_score,
    COALESCE(b.pass_count, 0) AS pass_count,
    COALESCE(b.pass_avg_score, 0) AS pass_avg_score,
    COALESCE(a.fail_count, 0) - COALESCE(b.pass_count, 0) AS fail_count_minus_pass_count
FROM fail_by_course a
FULL OUTER JOIN pass_by_course b
  ON a.course_id = b.course_id
ORDER BY course_id
