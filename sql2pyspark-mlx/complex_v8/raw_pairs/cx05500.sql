WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ABS(score - 50.9) AS score_distance
    FROM enrollments
    WHERE term <> '2024T1'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        score_distance,
        CASE WHEN score_distance >= 80.0 THEN 'HIGH' WHEN score_distance >= 35.0 THEN 'MEDIUM' ELSE 'LOW' END AS score_distance_tier,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label
    FROM cleaned_enrollments
    WHERE course_id <= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        score_distance,
        score_distance_tier,
        term_prefix,
        DENSE_RANK() OVER (ORDER BY score DESC NULLS FIRST, id) AS score_dense_rank,
        LAG(score_distance, 2) OVER (PARTITION BY course_id, score_distance_tier ORDER BY score DESC, id) AS prev2_score_distance,
        ROW_NUMBER() OVER (ORDER BY score DESC NULLS FIRST, id) AS rn,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score
    FROM scored
)
SELECT
    id,
    student_id,
    score,
    score_dense_rank,
    prev2_score_distance,
    SUBSTRING(TRIM(term_prefix), 1, 3) AS term_prefix_prefix,
    ABS(course_id_avg_score - 45.0) AS course_id_avg_score_distance
FROM windowed
WHERE course_id IS NOT NULL
ORDER BY score, id
LIMIT 5
