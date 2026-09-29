WITH fail_by_student AS (
    -- FAIL rows per student_id
    SELECT student_id, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY student_id
),
withdrawn_by_student AS (
    -- WITHDRAWN rows per student_id
    SELECT student_id, COUNT(*) AS withdrawn_count, ROUND(AVG(score), 2) AS withdrawn_avg_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY student_id
)
SELECT
    COALESCE(a.student_id, b.student_id) AS student_id,
    COALESCE(a.fail_count, 0) AS fail_count,
    a.fail_total_score,
    COALESCE(b.withdrawn_count, 0) AS withdrawn_count,
    b.withdrawn_avg_score,
    COALESCE(a.fail_count, 0) - COALESCE(b.withdrawn_count, 0) AS net_count
FROM fail_by_student a
FULL OUTER JOIN withdrawn_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
