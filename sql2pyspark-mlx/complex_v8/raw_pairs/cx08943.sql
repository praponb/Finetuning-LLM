WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CASE WHEN score >= 62.5 THEN 'A' WHEN score >= 34.2 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id <= 11
      AND grade_status IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        grade_status,
        term,
        SUM(days_since_enrolled_at) AS total_days_since_enrolled_at,
        SUM(CASE WHEN term IN ('2025T1', '2024T1') THEN 1 ELSE 0 END) AS n_term_in_2025t1_2024t1,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ROUND(100 * COUNT(CASE WHEN grade_status NOT IN ('INCOMPLETE', 'PASS') THEN 1 END) / COUNT(*), 1) AS pct_grade_status_not_in_incomple
    FROM scoped_enrollments
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), grade_status, term
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', enrolled_at_month) AS enrolled_at_month_year,
        ABS(n_term_in_2025t1_2024t1 - 5) AS n_term_in_2025t1_2024t1_distance,
        ABS(n_term_in_2025t1_2024t1 - 6) AS n_term_in_2025t1_2024t1_distance_2
    FROM summary
    WHERE n_term_in_2025t1_2024t1 < 7
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(enrolled_at_month) IGNORE NULLS OVER (PARTITION BY term ORDER BY n_term_in_2025t1_2024t1_distance_2 DESC, enrolled_at_month, grade_status, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_month,
        LEAD(total_days_since_enrolled_at) OVER w_term_by_n_term_in_2025t1_202_2 AS next_total_days_since_enrolled_at,
        COUNT(*) OVER (PARTITION BY n_term_in_2025t1_2024t1) AS rows_in_n_term_in_2025t1_2024t1,
        NTH_VALUE(enrolled_at_month, 2) OVER (PARTITION BY term ORDER BY n_term_in_2025t1_2024t1_distance_2 DESC, enrolled_at_month, grade_status, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_month,
        SUM(n_term_in_2025t1_2024t1) OVER (PARTITION BY n_term_in_2025t1_2024t1) AS n_term_in_2025t1_2024t1_sum_n_term_in_2025t1_202
    FROM enriched
    WINDOW w_term_by_n_term_in_2025t1_202_2 AS (PARTITION BY term ORDER BY n_term_in_2025t1_2024t1_distance_2 DESC, enrolled_at_month, grade_status, term)
)
-- Subtotals with GROUPING SETS
SELECT
    n_term_in_2025t1_2024t1,
    term,
    GROUPING(n_term_in_2025t1_2024t1) AS is_total_n_term_in_2025t1_2024t1,
    ROUND(AVG(n_term_in_2025t1_2024t1_distance_2), 2) AS avg_n_term_in_2025t1_2024t1_distance_2,
    ROUND(100 * COUNT(CASE WHEN term IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_term_is_not_null,
    COUNT(*) FILTER (WHERE grade_status <> 'WITHDRAWN') AS n_grade_status_withdrawn
FROM with_trends
GROUP BY GROUPING SETS ((n_term_in_2025t1_2024t1, term), (n_term_in_2025t1_2024t1), ())
ORDER BY n_term_in_2025t1_2024t1 NULLS LAST, term NULLS LAST
