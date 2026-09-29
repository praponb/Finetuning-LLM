WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        score,
        score_bucket,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM filtered
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        score,
        days_since_enrolled_at,
        ABS(score_bucket - 30) AS score_bucket_distance,
        ABS(days_since_enrolled_at - 350) AS days_since_enrolled_at_distance,
        ABS(score - 32.7) AS score_distance
    FROM enriched
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        score,
        score_bucket_distance,
        score_distance,
        ROUND(score_bucket_distance / NULLIF(score, 0), 3) AS score_bucket_distance_per_score,
        GREATEST(score, score_distance) AS greatest_score_score_distance
    FROM scored
    WHERE score_bucket_distance >= 35
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        score,
        score_bucket_distance,
        score_distance,
        score_bucket_distance_per_score,
        greatest_score_score_distance,
        FLOOR(score_distance / 25) * 25 AS score_distance_bucket
    FROM labeled
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        score_bucket_distance,
        score_bucket_distance_per_score,
        greatest_score_score_distance,
        score_distance_bucket,
        ROUND(CUME_DIST() OVER (ORDER BY score_bucket_distance_per_score ASC), 3) AS score_bucket_distance_per_score_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_score_bucket_distance_2, 3) AS score_bucket_distance_per_score_percent_rank
    FROM tagged
    WINDOW w_all_by_score_bucket_distance_2 AS (ORDER BY score_bucket_distance_per_score ASC, id)
)
-- Final projection
SELECT
    id,
    score_bucket_distance,
    score_bucket_distance_per_score,
    score_bucket_distance_per_score_cume_dist,
    FLOOR(score_bucket_distance / 25) * 25 AS score_bucket_distance_bucket,
    FLOOR(score_bucket_distance / 25) * 25 AS score_bucket_distance_bucket_2
FROM ranked
ORDER BY score_bucket_distance_per_score
