WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        FLOOR(score / 25) * 25 AS score_bucket,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 15:00:00'
      AND student_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN score_bucket >= 80 THEN 'HIGH' WHEN score_bucket >= 35 THEN 'MEDIUM' ELSE 'LOW' END AS score_bucket_tier
    FROM scoped
    WHERE grade_status_prefix IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY term ORDER BY score_bucket DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        ROUND(AVG(score_bucket) OVER w_score_bucket_tier, 2) AS score_bucket_tier_avg_score_bucket
    FROM derived
    WINDOW w_score_bucket_tier AS (PARTITION BY score_bucket_tier)
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 5
        )
      AND s.grade_status_term_label IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER w_grade_status_course_id, 0), 4) AS score_bucket_share_of_grade_status_course_id,
        ROUND(AVG(score) OVER (PARTITION BY score_bucket_tier ORDER BY score_bucket_tier_avg_score_bucket DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY score_bucket_tier ORDER BY score_bucket_tier_avg_score_bucket DESC), 3) AS score_bucket_tier_avg_score_bucket_percent_rank,
        score - LAG(score) OVER w_term_by_score_avg_2 AS score_change,
        LAG(score_avg_2, 2) OVER (PARTITION BY score_bucket_tier ORDER BY score_bucket_tier_avg_score_bucket DESC, id) AS prev2_score_avg_2
    FROM with_activity
    WINDOW w_grade_status_course_id AS (PARTITION BY grade_status, course_id), w_term_by_score_avg_2 AS (PARTITION BY term ORDER BY score_avg_2, id)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(CASE WHEN grade_status <> 'WITHDRAWN' THEN 1 END) AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM windowed s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    score_bucket,
    grade_status_term_label,
    score_bucket_tier,
    score_avg_2,
    score_bucket_share_of_grade_status_course_id,
    score_change,
    prev2_score_avg_2,
    enrollment_count,
    last_enrollment_enrolled_at,
    enrollment_grade_status_count,
    CONCAT_WS(' / ', grade_status_prefix, score_bucket_tier) AS grade_status_prefix_score_bucket_tier_label
FROM profiled
WHERE enrollment_grade_status_count > 3
ORDER BY score_bucket NULLS LAST, grade_status_term_label
