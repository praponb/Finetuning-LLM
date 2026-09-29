WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE course_id <= 10
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE grade_status <> 'PASS') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM valid_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(score_bucket - 40) AS score_bucket_distance,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        INITCAP(TRIM(term)) AS term_clean
    FROM with_counts
),
unique_rows AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_bucket,
        enrollment_grade_status_count,
        score_bucket_distance,
        grade_status_clean,
        term_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrollment_grade_status_count DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    score_bucket,
    score_bucket_distance,
    CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
    CASE WHEN score_bucket >= 85 THEN 'HIGH' WHEN score_bucket >= 35 THEN 'MEDIUM' ELSE 'LOW' END AS score_bucket_tier
FROM unique_rows
ORDER BY score_bucket_tier DESC
