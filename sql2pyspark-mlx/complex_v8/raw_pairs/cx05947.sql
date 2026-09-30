WITH fail_monthly AS (
    -- FAIL rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS fail_count, SUM(score) AS fail_total_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
),
withdrawn_monthly AS (
    -- WITHDRAWN rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS withdrawn_count, SUM(score) AS withdrawn_total_score
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.enrolled_at_month, b.enrolled_at_month) AS enrolled_at_month,
    COALESCE(a.fail_count, 0) AS fail_count,
    a.fail_total_score,
    COALESCE(b.withdrawn_count, 0) AS withdrawn_count,
    b.withdrawn_total_score,
    COALESCE(a.fail_count, 0) - COALESCE(b.withdrawn_count, 0) AS net_count
FROM fail_monthly a
FULL OUTER JOIN withdrawn_monthly b
  ON a.enrolled_at_month = b.enrolled_at_month
ORDER BY enrolled_at_month
