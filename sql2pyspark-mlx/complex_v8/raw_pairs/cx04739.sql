WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 69.6 THEN 'HIGH' WHEN score >= 20.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE course_id >= 4
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_bucket,
        LAG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_score,
        LAG(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev2_enrolled_at,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY TO_DATE(enrolled_at))) AS distinct_grade_status_in_enrolled_at_to_date,
        LAG(score_bucket) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_score_bucket,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY TO_DATE(enrolled_at)), 0), 4) AS score_bucket_share_of_enrolled_at_to_date,
        LAST_VALUE(score_bucket) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_bucket
    FROM recent_enrollments
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        term,
        score_bucket,
        prev_score,
        prev2_enrolled_at,
        distinct_grade_status_in_enrolled_at_to_date,
        prev_score_bucket,
        score_bucket_share_of_enrolled_at_to_date,
        ROUND(prev_score * 1.5, 2) AS prev_score_adj,
        CASE WHEN score_bucket >= 55 THEN 'A' WHEN score_bucket >= 40 THEN 'B' ELSE 'C' END AS score_bucket_tier
    FROM with_windows
    WHERE score >= 6.2
)
SELECT id, metric_name, reading
FROM (
    SELECT id, score_bucket_share_of_enrolled_at_to_date, prev_score, prev_score_adj
    FROM labeled
) src
UNPIVOT (
    reading FOR metric_name IN (score_bucket_share_of_enrolled_at_to_date, prev_score, prev_score_adj)
) unp
ORDER BY id, metric_name
