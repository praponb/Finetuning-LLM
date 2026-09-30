WITH pass_by_student AS (
    -- PASS rows per student_id
    SELECT student_id, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
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
    COALESCE(a.pass_count, 0) AS pass_count,
    a.pass_avg_score,
    COALESCE(b.withdrawn_count, 0) AS withdrawn_count,
    b.withdrawn_avg_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.withdrawn_count, 0) AS net_count
FROM pass_by_student a
FULL OUTER JOIN withdrawn_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
