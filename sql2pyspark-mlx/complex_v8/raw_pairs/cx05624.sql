WITH pass_monthly AS (
    -- PASS rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS pass_count, ROUND(AVG(score), 2) AS pass_avg_score
    FROM enrollments
    WHERE grade_status = 'PASS'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
),
fail_monthly AS (
    -- FAIL rows per enrolled_at_month
    SELECT DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month, COUNT(*) AS fail_count, ROUND(AVG(score), 2) AS fail_avg_score
    FROM enrollments
    WHERE grade_status = 'FAIL'
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.enrolled_at_month, b.enrolled_at_month) AS enrolled_at_month,
    COALESCE(a.pass_count, 0) AS pass_count,
    COALESCE(a.pass_avg_score, 0) AS pass_avg_score,
    COALESCE(b.fail_count, 0) AS fail_count,
    COALESCE(b.fail_avg_score, 0) AS fail_avg_score,
    COALESCE(a.pass_count, 0) - COALESCE(b.fail_count, 0) AS pass_count_minus_fail_count
FROM pass_monthly a
FULL OUTER JOIN fail_monthly b
  ON a.enrolled_at_month = b.enrolled_at_month
ORDER BY enrolled_at_month
