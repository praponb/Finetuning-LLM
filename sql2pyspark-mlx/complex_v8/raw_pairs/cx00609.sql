WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 38.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE score >= 36.6
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        score,
        grade_status,
        enrolled_at,
        score_adj,
        score_filled,
        score_tier,
        FLOOR(score / 25) * 25 AS score_bucket,
        SUBSTRING(TRIM(score_tier), 1, 3) AS score_tier_prefix,
        ABS(score_adj - 52.5) AS score_adj_distance
    FROM valid_enrollments
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE score_bucket > (SELECT MIN(score_bucket) FROM tagged)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        score,
        enrolled_at,
        score_filled,
        score_tier,
        score_tier_prefix,
        score_adj_distance,
        GREATEST(score_bucket, score_adj) AS greatest_score_bucket_score_adj,
        COALESCE(score_filled, 0.0) AS score_filled_filled
    FROM filtered
    WHERE score_filled <= 90.0
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        enrolled_at,
        score_tier,
        score_tier_prefix,
        score_adj_distance,
        score_filled_filled,
        ROUND(score / NULLIF(score_filled, 0), 3) AS score_per_score_filled,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM derived
)
SELECT
    id,
    score_tier,
    score_adj_distance,
    score_filled_filled,
    score_per_score_filled,
    enrolled_at_date,
    CASE WHEN score_adj_distance >= 97.5 THEN 'A' WHEN score_adj_distance >= 45.0 THEN 'B' ELSE 'C' END AS score_adj_distance_tier,
    CONCAT_WS('|', score_tier, score_tier_prefix) AS score_tier_score_tier_prefix_label
FROM enriched
WHERE score_tier_prefix IS NOT NULL
ORDER BY score_adj_distance, score_tier DESC, id
LIMIT 5
