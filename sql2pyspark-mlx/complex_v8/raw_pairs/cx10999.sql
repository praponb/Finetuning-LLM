WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 62.5 THEN 'A' WHEN score >= 34.2 THEN 'B' ELSE 'C' END AS score_tier,
        FLOOR(score / 25) * 25 AS score_bucket,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE course_id >= 10
),
totals AS (
    -- Group-level totals
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        grade_status,
        score_tier,
        MIN(score) AS min_score,
        SUM(CASE WHEN term IN ('2025T1', '2024T1', '2025T2') THEN 1 ELSE 0 END) AS n_term_in_2025t1_2024t1_2025t2
    FROM base_enrollments
    GROUP BY TO_DATE(enrolled_at), grade_status, score_tier
),
annotated AS (
    -- Label and score rows
    SELECT
        enrolled_at_day,
        grade_status,
        score_tier,
        n_term_in_2025t1_2024t1_2025t2,
        DATE_ADD(enrolled_at_day, 30) AS enrolled_at_day_plus_30d,
        DATEDIFF(DATE '2025-12-31', enrolled_at_day) AS days_since_enrolled_at_day
    FROM totals
    WHERE min_score <= 55.0
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE days_since_enrolled_at_day > (SELECT AVG(days_since_enrolled_at_day) FROM annotated)
)
SELECT enrolled_at_day, grade_status, metric, metric_value
FROM (
    SELECT enrolled_at_day, grade_status, n_term_in_2025t1_2024t1_2025t2, days_since_enrolled_at_day
    FROM without_matches
) src
UNPIVOT (
    metric_value FOR metric IN (n_term_in_2025t1_2024t1_2025t2 AS m_n_term_in_2025t1_2024t1_2025t2, days_since_enrolled_at_day AS m_days_since_enrolled_at_day)
) unp
ORDER BY enrolled_at_day, grade_status, metric
