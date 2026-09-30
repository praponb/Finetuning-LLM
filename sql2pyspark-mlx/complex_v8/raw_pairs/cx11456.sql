WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        CASE WHEN score >= 57.6 THEN 'A' WHEN score >= 20.2 THEN 'B' ELSE 'C' END AS score_tier,
        ABS(score - 42.4) AS score_distance
    FROM enrollments
    WHERE term = '2025T1'
      AND student_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(score) OVER (PARTITION BY score_tier ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        LEAD(score_distance, 2) OVER (PARTITION BY YEAR(enrolled_at), grade_status ORDER BY score, id) AS next2_score_distance,
        MIN(score) OVER w_student_id AS student_id_min_score,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY student_id), 0), 4) AS score_distance_share_of_student_id
    FROM base_enrollments
    WINDOW w_student_id AS (PARTITION BY student_id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, ROUND(score_distance * 0.9, 2) AS score_distance_adj
    FROM sequenced
    WHERE last_score > 38.3
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    term_prefix,
    score_tier,
    score_distance,
    last_score,
    next2_score_distance,
    student_id_min_score,
    score_distance_share_of_student_id,
    score_distance_adj
FROM enriched
WHERE student_id IS NOT NULL
ORDER BY student_id DESC, next2_score_distance
