WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 73.7 THEN 'HIGH' WHEN score >= 35.8 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM valid_enrollments s
),
term_stats AS (
    -- Avg score_bucket per term
    SELECT term, AVG(score_bucket) AS avg_score_bucket
    FROM summarized_rows
    GROUP BY term
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_bucket,
        ROUND(s.score_bucket - g.avg_score_bucket, 2) AS score_bucket_minus_avg
    FROM summarized_rows s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score_bucket > g.avg_score_bucket
)
SELECT
    COALESCE(score_tier, 'ALL') AS score_tier,
    student_id,
    COALESCE(term, 'ALL') AS term,
    ROUND(AVG(score_bucket), 2) AS avg_score_bucket_2,
    SUM(score_bucket) AS total_score_bucket
FROM compared
GROUP BY GROUPING SETS ((score_tier, student_id, term), (score_tier), ())
ORDER BY score_tier NULLS LAST, student_id NULLS LAST, term NULLS LAST
