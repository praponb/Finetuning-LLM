WITH pass_by_student AS (
    -- PASS rows per student_id
    SELECT student_id, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY student_id
),
fail_by_student AS (
    -- FAIL rows per student_id
    SELECT student_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY student_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.student_id, b.student_id) AS student_id,
    COALESCE(a.pass_count, 0) AS pass_count,
    COALESCE(a.pass_avg_score, 0) AS pass_avg_score,
    COALESCE(b.fail_count, 0) AS fail_count,
    COALESCE(b.fail_total_score, 0) AS fail_total_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.fail_count, 0) AS net_count
FROM pass_by_student a
FULL OUTER JOIN fail_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
