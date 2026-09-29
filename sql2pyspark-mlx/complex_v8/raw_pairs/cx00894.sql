WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE course_id >= 10
),
summary AS (
    -- Group-level totals
    SELECT
        term,
        YEAR(enrolled_at) AS enrolled_at_year,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_incomplete,
        SUM(CASE WHEN term IS NOT NULL THEN 1 ELSE 0 END) AS n_term_is_not_null,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null_2
    FROM scoped
    WHERE HOUR(enrolled_at) BETWEEN 7 AND 11
    GROUP BY 1, 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        term,
        enrolled_at_year,
        n_term_is_not_null,
        n_term_is_not_null_2 + n_term_is_not_null AS n_term_is_not_null_2_plus_n_term_is_not_null,
        INITCAP(TRIM(term)) AS term_clean
    FROM summary
    WHERE enrolled_at_year <= 2026
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE n_term_is_not_null_2_plus_n_term_is_not_null > (SELECT AVG(n_term_is_not_null_2_plus_n_term_is_not_null) FROM tagged)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY n_term_is_not_null)) AS distinct_term_in_n_term_is_not_null,
        ROUND(AVG(n_term_is_not_null) OVER (PARTITION BY n_term_is_not_null), 2) AS n_term_is_not_null_avg_n_term_is_not_null
    FROM qualified
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        term,
        enrolled_at_year,
        n_term_is_not_null_2_plus_n_term_is_not_null,
        distinct_term_in_n_term_is_not_null,
        n_term_is_not_null_avg_n_term_is_not_null,
        SUM(enrolled_at_year) OVER (PARTITION BY term) AS term_sum_enrolled_at_year,
        MIN(enrolled_at_year) OVER (PARTITION BY term) AS term_min_enrolled_at_year
    FROM ranked
),
enriched AS (
    -- Label and score rows
    SELECT
        term,
        enrolled_at_year,
        n_term_is_not_null_2_plus_n_term_is_not_null,
        distinct_term_in_n_term_is_not_null,
        n_term_is_not_null_avg_n_term_is_not_null,
        term_sum_enrolled_at_year,
        term_min_enrolled_at_year,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM with_trends
    WHERE term <> '2025T2'
)
-- Final projection
SELECT
    term,
    enrolled_at_year,
    distinct_term_in_n_term_is_not_null,
    term_sum_enrolled_at_year,
    term_min_enrolled_at_year
FROM enriched
ORDER BY enrolled_at_year, term_sum_enrolled_at_year DESC
