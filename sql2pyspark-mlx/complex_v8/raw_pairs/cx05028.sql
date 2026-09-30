WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 59.7 THEN 'large' WHEN score >= 32.7 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE course_id IS NOT NULL
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.score_bucket,
        s.score_tier,
        c.dept AS course_dept,
        c.level AS course_level
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY score_bucket DESC) AS score_bucket_rank,
        score_bucket - LAG(score_bucket) OVER w_all_by_score_bucket AS score_bucket_change,
        ROUND(CUME_DIST() OVER w_all_by_score_bucket_ties_2, 3) AS score_bucket_cume_dist,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        SIZE(COLLECT_SET(grade_status) OVER w_course_level) AS distinct_grade_status_in_course_level,
        LAG(score_bucket) OVER (ORDER BY score_bucket DESC, id) AS prev_score_bucket,
        LAG(score_bucket, 3) OVER (ORDER BY score_bucket DESC, id) AS prev3_score_bucket,
        ROUND(score_bucket - AVG(score_bucket) OVER (PARTITION BY course_level), 2) AS score_bucket_vs_course_level_avg,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY course_level), 0), 4) AS score_bucket_share_of_course_level,
        RANK() OVER (ORDER BY score_bucket) AS score_bucket_rank_2,
        SUM(score_bucket) OVER w_course_level AS course_level_sum_score_bucket,
        LAST_VALUE(score_bucket) OVER (ORDER BY score_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_bucket
    FROM joined_course
    WINDOW w_all_by_score_bucket AS (ORDER BY score_bucket, id), w_all_by_score_bucket_ties_2 AS (ORDER BY score_bucket), w_course_level AS (PARTITION BY course_level)
)
SELECT
    id,
    student_id,
    grade_status,
    score_bucket,
    score_tier,
    course_level,
    score_bucket_cume_dist,
    distinct_grade_status_in_course_level,
    prev_score_bucket,
    score_bucket_vs_course_level_avg,
    score_bucket_rank_2,
    course_level_sum_score_bucket,
    last_score_bucket
FROM ranked
WHERE score_tier IS NOT NULL
ORDER BY score_bucket_rank_2, grade_status DESC NULLS FIRST, id
LIMIT 5
