WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        HOUR(enrolled_at) AS enrolled_at_hour,
        CASE WHEN score >= 71.0 THEN 'HIGH' WHEN score >= 42.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE student_id <= 6
      AND term IN ('2025T2', '2024T1')
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.score_bucket,
        s.enrolled_at_hour,
        s.score_tier,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM recent s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY score_tier ORDER BY enrolled_at, id, enrollment_id) AS enrolled_at_seq,
        SIZE(COLLECT_SET(enrollment_id) OVER (PARTITION BY score_tier, course_id)) AS distinct_enrollment_id_in_score_tier_course_id,
        score_bucket - LAG(score_bucket) OVER w_score_tier_by_score_bucket AS score_bucket_change,
        ROUND(CUME_DIST() OVER w_score_tier_by_enrolled_at, 3) AS enrolled_at_cume_dist,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY score_tier, course_id), 0), 4) AS score_bucket_share_of_score_tier_course_id
    FROM detail_rows
    WINDOW w_score_tier_by_score_bucket AS (PARTITION BY score_tier ORDER BY score_bucket, id, enrollment_id), w_score_tier_by_enrolled_at AS (PARTITION BY score_tier ORDER BY enrolled_at, id, enrollment_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT score_tier, enrolled_at, enrollment_grade_status, score_bucket_change
    FROM with_windows
) src
PIVOT (
    MAX(score_bucket_change)
    FOR enrollment_grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS', 'FAIL')
)
