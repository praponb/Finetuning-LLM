WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE (enrolled_at >= TIMESTAMP '2025-01-19 15:00:00' OR term NOT IN ('2025T2', '2024T1'))
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(score, 2) OVER (PARTITION BY grade_status_or_unknown ORDER BY score ASC, id) AS prev2_score,
        MAX(score) OVER (PARTITION BY student_id, term) AS student_id_term_max_score,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status_or_unknown ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        MIN(score) OVER (PARTITION BY student_id, term) AS student_id_term_min_score,
        ROUND(CUME_DIST() OVER w_grade_status_or_unknown_by_s, 3) AS score_cume_dist
    FROM recent_enrollments
    WINDOW w_grade_status_or_unknown_by_s AS (PARTITION BY grade_status_or_unknown ORDER BY score ASC, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY prev2_score ASC, id) AS prev2_score_quintile,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY prev2_score ASC, id) AS prev2_score_quartile,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY prev2_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LAG(enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY prev2_score ASC, id) AS prev_enrolled_at,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY TO_DATE(enrolled_at))) AS distinct_term_in_enrolled_at_to_date
    FROM windowed
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(term)) AS term_len,
        UPPER(TRIM(enrolled_at_label)) AS enrolled_at_label_clean
    FROM windowed_2
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_term_in_enrolled_at_to_date,
    course_id,
    GROUPING(distinct_term_in_enrolled_at_to_date) AS is_total_distinct_term_in_enrolled_at_to_date,
    ROUND(STDDEV(score_cume_dist), 2) AS stddev_score_cume_dist
FROM labeled
GROUP BY GROUPING SETS ((distinct_term_in_enrolled_at_to_date, course_id), (distinct_term_in_enrolled_at_to_date), ())
ORDER BY distinct_term_in_enrolled_at_to_date NULLS LAST, course_id NULLS LAST
