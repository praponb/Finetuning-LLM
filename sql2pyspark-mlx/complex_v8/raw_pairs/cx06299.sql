WITH withdrawn_by_student AS (
    -- WITHDRAWN rows per student_id
    SELECT student_id, COUNT(*) AS withdrawn_count, SUM(score) AS withdrawn_total_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY student_id
),
fail_by_student AS (
    -- FAIL rows per student_id
    SELECT student_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY student_id
)
SELECT
    COALESCE(a.student_id, b.student_id) AS student_id,
    COALESCE(a.withdrawn_count, 0) AS withdrawn_count,
    COALESCE(a.withdrawn_total_score, 0) AS withdrawn_total_score,
    COALESCE(b.fail_count, 0) AS fail_count,
    b.fail_total_score,
    COALESCE(a.withdrawn_count, 0) - COALESCE(b.fail_count, 0) AS withdrawn_count_minus_fail_count
FROM withdrawn_by_student a
FULL OUTER JOIN fail_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
