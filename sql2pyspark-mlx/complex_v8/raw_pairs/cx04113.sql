WITH withdrawn_monthly AS (
    -- WITHDRAWN rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS withdrawn_count, ROUND(AVG(score), 2) AS withdrawn_avg_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
),
pass_monthly AS (
    -- PASS rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS pass_count, SUM(score) AS pass_total_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.enrolled_at_month, b.enrolled_at_month) AS enrolled_at_month,
    COALESCE(a.withdrawn_count, 0) AS withdrawn_count,
    COALESCE(a.withdrawn_avg_score, 0) AS withdrawn_avg_score,
    COALESCE(b.pass_count, 0) AS pass_count,
    b.pass_total_score,
    COALESCE(a.withdrawn_count, 0) - COALESCE(b.pass_count, 0) AS net_count
FROM withdrawn_monthly a
FULL OUTER JOIN pass_monthly b
  ON a.enrolled_at_month = b.enrolled_at_month
ORDER BY enrolled_at_month
