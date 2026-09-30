WITH withdrawn_by_course AS (
    -- WITHDRAWN rows per course_id
    SELECT course_id, COUNT(*) AS withdrawn_count, ROUND(AVG(score), 2) AS withdrawn_avg_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY course_id
),
fail_by_course AS (
    -- FAIL rows per course_id
    SELECT course_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY course_id
)
SELECT
    COALESCE(a.course_id, b.course_id) AS course_id,
    COALESCE(a.withdrawn_count, 0) AS withdrawn_count,
    COALESCE(a.withdrawn_avg_score, 0) AS withdrawn_avg_score,
    COALESCE(b.fail_count, 0) AS fail_count,
    b.fail_total_score,
    COALESCE(a.withdrawn_count, 0) - COALESCE(b.fail_count, 0) AS withdrawn_count_minus_fail_count
FROM withdrawn_by_course a
FULL OUTER JOIN fail_by_course b
  ON a.course_id = b.course_id
ORDER BY course_id
