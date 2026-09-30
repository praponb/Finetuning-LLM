WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN score >= 69.6 THEN 'A' WHEN score >= 23.7 THEN 'B' ELSE 'C' END AS score_tier,
        CASE WHEN score >= 63.9 THEN 'HIGH' WHEN score >= 38.1 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier_2,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE course_id <= 7
),
latest AS (
    -- Keep the latest row per grade_status, score_tier
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_tier,
        score_tier_2,
        grade_status_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, score_tier ORDER BY score DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist,
        FIRST_VALUE(score) OVER (PARTITION BY course_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        score - LAG(score) OVER w_course_id_by_score AS score_change,
        ROUND(AVG(score) OVER (ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7
    FROM latest
    WINDOW w_all_by_score_ties AS (ORDER BY score), w_course_id_by_score AS (PARTITION BY course_id ORDER BY score ASC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT score_tier, grade_status, score_tier_2, first_score
    FROM with_trends
) src
PIVOT (
    MAX(first_score)
    FOR score_tier_2 IN ('HIGH', 'MEDIUM', 'LOW')
)
ORDER BY score_tier, grade_status
