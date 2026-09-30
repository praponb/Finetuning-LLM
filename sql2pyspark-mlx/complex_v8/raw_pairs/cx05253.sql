WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status NOT IN ('WITHDRAWN', 'FAIL')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score_filled,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        CASE WHEN score >= 73.0 THEN 'HIGH' WHEN score >= 34.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM filtered
    WHERE grade_status <> 'PASS'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at_quarter,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at_quarter NULLS LAST, id) AS rn,
        ROUND(AVG(enrolled_at_quarter) OVER (PARTITION BY student_id ORDER BY enrolled_at_quarter NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS enrolled_at_quarter_avg_4
    FROM scored
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.term IN ('2025T1', '2024T2', '2024T1') THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_term_count,
        (SELECT COUNT(CASE WHEN e.term IS NOT NULL THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_term_count_2,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM windowed s
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(enrollment_term_count_2 / NULLIF(enrollment_term_count, 0), 3) AS enrollment_term_count_2_per_enrollment_term_coun,
        ABS(enrollment_term_count_2 - 3) AS enrollment_term_count_2_distance,
        ROUND(rn / NULLIF(enrolled_at_quarter, 0), 3) AS rn_per_enrolled_at_quarter
    FROM profiled
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at_quarter,
    rn,
    enrolled_at_quarter_avg_4,
    enrollment_term_count,
    enrollment_term_count_2,
    last_enrollment_enrolled_at,
    enrollment_term_count_2_per_enrollment_term_coun,
    enrollment_term_count_2_distance,
    rn_per_enrolled_at_quarter,
    FLOOR(rn / 2) * 2 AS rn_bucket,
    ABS(enrollment_term_count - 3) AS enrollment_term_count_distance
FROM labeled
ORDER BY enrolled_at_quarter, id
LIMIT 10
