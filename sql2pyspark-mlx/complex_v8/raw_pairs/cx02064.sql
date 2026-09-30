WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE (student_id <= 8 OR course_id IS NOT NULL)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM scoped_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        score,
        grade_status,
        enrolled_at_week,
        total_enrollment_score,
        CASE WHEN score >= 58.8 THEN 'large' WHEN score >= 13.9 THEN 'medium' ELSE 'small' END AS score_tier,
        score + total_enrollment_score AS score_plus_total_enrollment_score
    FROM profiled
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        grade_status,
        score_tier,
        score_plus_total_enrollment_score,
        CASE WHEN grade_status IN ('FAIL', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM labeled
)
SELECT
    id,
    grade_status,
    score_tier,
    score_plus_total_enrollment_score,
    CASE WHEN score_tier IN ('large', 'small') THEN 'group_a' WHEN score_tier = 'medium' THEN 'group_b' ELSE 'other' END AS score_tier_group,
    UPPER(TRIM(score_tier)) AS score_tier_clean
FROM scored
ORDER BY id NULLS LAST, score_plus_total_enrollment_score
