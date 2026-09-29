WITH withdrawn_by_student AS (
    -- WITHDRAWN rows per student_id
    SELECT student_id, COUNT(*) AS withdrawn_count, SUM(score) AS withdrawn_total_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
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
    COALESCE(a.withdrawn_count, 0) AS withdrawn_count,
    a.withdrawn_total_score,
    COALESCE(b.pass_count, 0) AS pass_count,
    b.pass_total_score,
    COALESCE(a.withdrawn_count, 0) - COALESCE(b.pass_count, 0) AS net_count
FROM withdrawn_by_student a
FULL OUTER JOIN pass_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
