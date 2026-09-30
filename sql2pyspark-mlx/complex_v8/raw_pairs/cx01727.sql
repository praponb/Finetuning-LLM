WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at_2,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE term IN ('2025T2', '2024T1', '2025T1')
      AND enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
term_stats AS (
    -- Avg days_since_enrolled_at per term
    SELECT term, AVG(days_since_enrolled_at) AS avg_days_since_enrolled_at
    FROM prepared_enrollments
    GROUP BY term
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_enrolled_at,
        ROUND(s.days_since_enrolled_at - g.avg_days_since_enrolled_at, 2) AS days_since_enrolled_at_minus_avg
    FROM prepared_enrollments s
    JOIN term_stats g ON g.term = s.term
    WHERE s.days_since_enrolled_at > g.avg_days_since_enrolled_at
),
totals AS (
    -- Group-level totals
    SELECT
        grade_status,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) AS cnt,
        SUM(CASE WHEN grade_status = 'WITHDRAWN' THEN score_bucket ELSE 0 END) AS score_bucket_grade_status_withdrawn,
        MAX(score_bucket) AS max_score_bucket
    FROM outliers
    GROUP BY 1
),
per_label AS (
    -- One row per term_list_item
    SELECT
        s.grade_status,
        s.cnt,
        s.score_bucket_grade_status_withdrawn,
        s.max_score_bucket,
        t.term_list_item
    FROM totals s
    LATERAL VIEW OUTER EXPLODE(s.term_list) t AS term_list_item
)
SELECT
    grade_status,
    cnt,
    score_bucket_grade_status_withdrawn,
    max_score_bucket,
    term_list_item
FROM per_label
WHERE grade_status IS NOT NULL
ORDER BY grade_status DESC
