WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ABS(score - 50.9) AS score_distance,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
      AND course_id IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ROUND(100 * COUNT(CASE WHEN term IN ('2025T2', '2025T1', '2024T1') THEN 1 END) / COUNT(*), 1) AS pct_term_in_2025t2_2025t1_2024t1,
        ROUND(SUM(score_distance), 2) AS total_score_distance,
        ROUND(STDDEV(score), 2) AS stddev_score,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN term IN ('2025T2', '2025T1') THEN 1 END) / COUNT(*), 1) AS pct_term_in_2025t2_2025t1
    FROM cleaned_enrollments
    GROUP BY 1
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE pct_term_in_2025t2_2025t1_2024t1 > (SELECT MIN(pct_term_in_2025t2_2025t1_2024t1) FROM rollup_base)
)
SELECT enrolled_at_ym, metric_name, value
FROM (
    SELECT enrolled_at_ym, total_score_distance, pct_term_in_2025t2_2025t1, pct_term_in_2025t2_2025t1_2024t1, stddev_score
    FROM without_matches
) src
UNPIVOT (
    value FOR metric_name IN (total_score_distance, pct_term_in_2025t2_2025t1, pct_term_in_2025t2_2025t1_2024t1, stddev_score)
) unp
ORDER BY enrolled_at_ym, metric_name
