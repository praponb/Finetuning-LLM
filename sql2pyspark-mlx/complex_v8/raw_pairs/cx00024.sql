WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        CASE WHEN score >= 74.9 THEN 'A' WHEN score >= 38.3 THEN 'B' ELSE 'C' END AS score_tier,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE term = '2024T1'
      AND student_id <= 7
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score >= 39.4
              AND e.student_id >= 15
        )
      AND s.grade_status = 'PASS'
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.cohort AS student_cohort, c.dept AS course_dept
    FROM with_activity s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', enrolled_at_month) AS enrolled_at_month_month,
        term,
        ROUND(SUM(score) / NULLIF(SUM(days_since_enrolled_at), 0), 3) AS score_to_days_since_enrolled_at_ratio,
        ROUND(AVG(days_since_enrolled_at), 2) AS avg_days_since_enrolled_at,
        SUM(days_since_enrolled_at) AS total_days_since_enrolled_at
    FROM enriched_student
    GROUP BY DATE_TRUNC('MONTH', enrolled_at_month), term
    HAVING AVG(score) > 38.1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE score_to_days_since_enrolled_at_ratio > (SELECT AVG(score_to_days_since_enrolled_at_ratio) FROM rollup_base)
)
SELECT enrolled_at_month_month, term, field, metric_value
FROM (
    SELECT enrolled_at_month_month, term, score_to_days_since_enrolled_at_ratio, avg_days_since_enrolled_at
    FROM qualified
) src
UNPIVOT (
    metric_value FOR field IN (score_to_days_since_enrolled_at_ratio, avg_days_since_enrolled_at)
) unp
