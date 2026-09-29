WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CASE WHEN score >= 62.5 THEN 'HIGH' WHEN score >= 35.8 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE (score <= 90.9 OR HOUR(enrolled_at) BETWEEN 8 AND 14)
),
grouped AS (
    -- Group-level totals
    SELECT
        term_or_unknown,
        SUM(CASE WHEN grade_status <> 'WITHDRAWN' THEN 1 ELSE 0 END) AS n_grade_status_withdrawn,
        COUNT(*) AS cnt,
        ROUND(SUM(score_filled) / NULLIF(SUM(score), 0), 3) AS score_filled_to_score_ratio,
        ROUND(SUM(score), 2) AS total_score,
        PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM valid
    GROUP BY term_or_unknown
    HAVING SUM(score_filled) > 30.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT *, ROW_NUMBER() OVER w_all_by_total_score AS seq_no
    FROM grouped
    WINDOW w_all_by_total_score AS (ORDER BY total_score DESC, term_or_unknown)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE median_score > (SELECT MIN(median_score) FROM ranked)
)
-- Final projection
SELECT
    term_or_unknown,
    n_grade_status_withdrawn,
    cnt,
    total_score,
    median_score
FROM screened
ORDER BY cnt, median_score NULLS LAST
