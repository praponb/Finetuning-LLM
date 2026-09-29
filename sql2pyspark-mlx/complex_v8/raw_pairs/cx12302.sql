WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE (enrolled_at < TIMESTAMP '2025-03-08 11:00:00' OR course_id <= 7)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_bucket,
        MIN(score_bucket) OVER (PARTITION BY grade_status, MONTH(enrolled_at)) AS grade_status_enrolled_at_month_min_score_bucket,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY score DESC, id), 3) AS score_percent_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(CUME_DIST() OVER w_course_id_by_score_ties, 3) AS score_cume_dist,
        LEAD(score_bucket, 1, 0) OVER (PARTITION BY grade_status ORDER BY score_bucket, id) AS next_score_bucket,
        COUNT(*) OVER (PARTITION BY grade_status, MONTH(enrolled_at)) AS rows_in_grade_status_enrolled_at_month,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, MONTH(enrolled_at)), 0), 4) AS score_share_of_grade_status_enrolled_at_month,
        ROW_NUMBER() OVER w_all_by_score_bucket AS rn,
        FIRST_VALUE(score_bucket) OVER (PARTITION BY grade_status ORDER BY score_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_bucket,
        ROUND(AVG(score_bucket) OVER (PARTITION BY grade_status, MONTH(enrolled_at)), 2) AS grade_status_enrolled_at_month_avg_score_bucket,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY score_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        MAX(score_bucket) OVER w_grade_status_enrolled_at_mon AS grade_status_enrolled_at_month_max_score_bucket,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY grade_status, MONTH(enrolled_at)), 0), 4) AS score_bucket_share_of_grade_status_enrolled_at_m
    FROM valid
    WINDOW w_course_id_by_score_ties AS (PARTITION BY course_id ORDER BY score DESC), w_grade_status_enrolled_at_mon AS (PARTITION BY grade_status, MONTH(enrolled_at)), w_all_by_score_bucket AS (ORDER BY score_bucket ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_bucket,
        grade_status_enrolled_at_month_min_score_bucket,
        score_percent_rank,
        first_known_score,
        score_cume_dist,
        next_score_bucket,
        rows_in_grade_status_enrolled_at_month,
        score_share_of_grade_status_enrolled_at_month,
        first_score_bucket,
        grade_status_enrolled_at_month_avg_score_bucket,
        score_avg_3_prior,
        grade_status_enrolled_at_month_max_score_bucket,
        score_bucket_share_of_grade_status_enrolled_at_m
    FROM ranked
    WHERE rn <= 2
      AND score_share_of_grade_status_enrolled_at_month < 0.7
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    score_percent_rank,
    first_known_score,
    next_score_bucket,
    rows_in_grade_status_enrolled_at_month,
    score_share_of_grade_status_enrolled_at_month,
    first_score_bucket,
    grade_status_enrolled_at_month_avg_score_bucket,
    score_bucket_share_of_grade_status_enrolled_at_m,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
FROM leaders
ORDER BY grade_status_slug DESC
