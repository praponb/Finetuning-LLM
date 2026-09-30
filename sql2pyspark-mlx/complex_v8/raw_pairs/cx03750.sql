WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score < 85.2
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term_group,
        CASE WHEN score_bucket >= 70 THEN 'HIGH' WHEN score_bucket >= 25 THEN 'MEDIUM' ELSE 'LOW' END AS score_bucket_tier,
        CASE WHEN score_bucket >= 65 THEN 'HIGH' WHEN score_bucket >= 45 THEN 'MEDIUM' ELSE 'LOW' END AS score_bucket_tier_2,
        ABS(score - 63.9) AS score_distance
    FROM scoped
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score_bucket_tier,
        score_bucket_tier_2,
        SUM(score_distance) OVER (PARTITION BY student_id ORDER BY score_distance DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_distance_sum_7,
        NTH_VALUE(score_distance, 2) OVER (PARTITION BY student_id ORDER BY score_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_distance
    FROM enriched
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.score_bucket_tier,
        s.score_bucket_tier_2,
        s.second_score_distance,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id
    FROM sequenced s
    JOIN enrollments e ON e.student_id = s.student_id AND e.course_id IS NOT NULL
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM detail_rows_enrollments
    WHERE enrollment_score < (SELECT MAX(enrollment_score) FROM detail_rows_enrollments)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.score_bucket_tier,
        s.score_bucket_tier_2,
        s.second_score_distance,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_course_id,
        c.credits AS course_credits,
        c.course_code,
        e.score AS enrollment_score_2
    FROM qualified s
    JOIN courses c ON c.id = s.enrollment_course_id AND c.dept <> 'Bio'
    JOIN enrollments e ON e.id = s.enrollment_id
)
SELECT
    id,
    student_id,
    score_bucket_tier,
    score_bucket_tier_2,
    second_score_distance,
    enrollment_id,
    enrollment_score,
    enrollment_course_id,
    course_code,
    CASE WHEN score_bucket_tier_2 IN ('HIGH', 'LOW') THEN 'group_a' WHEN score_bucket_tier_2 = 'MEDIUM' THEN 'group_b' ELSE 'other' END AS score_bucket_tier_2_group,
    LOWER(REGEXP_REPLACE(TRIM(score_bucket_tier), ' ', '_')) AS score_bucket_tier_slug
FROM with_parent
WHERE student_id IS NOT NULL
ORDER BY score_bucket_tier_2
