WITH fail_by_student AS (
    -- FAIL rows per student_id
    SELECT student_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY student_id
),
pass_by_student AS (
    -- PASS rows per student_id
    SELECT student_id, COUNT(*) AS pass_count, SUM(score) AS pass_total_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY student_id
)
SELECT
    COALESCE(a.student_id, b.student_id) AS student_id,
    COALESCE(a.fail_count, 0) AS fail_count,
    a.fail_total_score,
    COALESCE(b.pass_count, 0) AS pass_count,
    COALESCE(b.pass_total_score, 0) AS pass_total_score,
    COALESCE(a.fail_count, 0) - COALESCE(b.pass_count, 0) AS fail_count_minus_pass_count
FROM fail_by_student a
FULL OUTER JOIN pass_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
