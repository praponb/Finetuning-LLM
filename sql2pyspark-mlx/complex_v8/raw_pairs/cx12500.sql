WITH pass_by_student AS (
    -- PASS rows per student_id
    SELECT student_id, COUNT(*) AS pass_count, SUM(score) AS pass_total_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY student_id
),
withdrawn_by_student AS (
    -- WITHDRAWN rows per student_id
    SELECT student_id, COUNT(*) AS withdrawn_count, SUM(score) AS withdrawn_total_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY student_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.student_id, b.student_id) AS student_id,
    COALESCE(a.pass_count, 0) AS pass_count,
    a.pass_total_score,
    COALESCE(b.withdrawn_count, 0) AS withdrawn_count,
    COALESCE(b.withdrawn_total_score, 0) AS withdrawn_total_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.withdrawn_count, 0) AS pass_count_minus_withdrawn_count
FROM pass_by_student a
FULL OUTER JOIN withdrawn_by_student b
  ON a.student_id = b.student_id
ORDER BY student_id
