WITH pass_by_course AS (
    -- PASS rows per course_id
    SELECT course_id, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY course_id
),
fail_by_course AS (
    -- FAIL rows per course_id
    SELECT course_id, COUNT(*) AS fail_count, ROUND(AVG(score), 2) AS fail_avg_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY course_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.course_id, b.course_id) AS course_id,
    COALESCE(a.pass_count, 0) AS pass_count,
    a.pass_avg_score,
    COALESCE(b.fail_count, 0) AS fail_count,
    COALESCE(b.fail_avg_score, 0) AS fail_avg_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.fail_count, 0) AS pass_count_minus_fail_count
FROM pass_by_course a
FULL OUTER JOIN fail_by_course b
  ON a.course_id = b.course_id
ORDER BY course_id
