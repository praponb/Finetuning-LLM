WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE term IN ('2025T1', '2025T2')
      AND student_id >= 6
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', term, grade_status_clean) AS term_grade_status_clean_label,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        LOWER(TRIM(grade_status_clean)) AS grade_status_clean_clean
    FROM scoped
    WHERE student_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(score, 0.0) AS score_filled,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM labeled
    WHERE term_or_unknown = 'UNKNOWN'
),
term_or_unknown_stats AS (
    -- Max score_filled per term_or_unknown
    SELECT term_or_unknown, MAX(score_filled) AS max_score_filled
    FROM tagged
    GROUP BY term_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_score_filled,
        ROUND(s.score_filled - g.max_score_filled, 2) AS score_filled_minus_max
    FROM tagged s
    JOIN term_or_unknown_stats g ON g.term_or_unknown = s.term_or_unknown
    WHERE s.score_filled = g.max_score_filled
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term = '2025T2'
        )
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        term_or_unknown,
        ROUND(SUM(score_filled) / NULLIF(SUM(days_since_enrolled_at), 0), 3) AS score_filled_to_days_since_enrolled_at_ratio,
        ROUND(AVG(days_since_enrolled_at), 2) AS avg_days_since_enrolled_at,
        MAX(enrolled_at) AS last_enrolled_at,
        MIN(enrolled_at) AS first_enrolled_at,
        SUM(CASE WHEN term NOT IN ('2024T2', '2024T1') THEN 1 ELSE 0 END) AS n_term_not_in_2024t2_2024t1,
        COUNT(DISTINCT term) AS distinct_terms
    FROM with_activity
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), term_or_unknown
    HAVING MAX(score_filled) > 10.0
)
SELECT
    enrolled_at_month,
    term_or_unknown,
    score_filled_to_days_since_enrolled_at_ratio,
    avg_days_since_enrolled_at,
    last_enrolled_at,
    first_enrolled_at,
    n_term_not_in_2024t2_2024t1,
    distinct_terms
FROM per_group
ORDER BY n_term_not_in_2024t2_2024t1 DESC, enrolled_at_month, term_or_unknown
LIMIT 5
