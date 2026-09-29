WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND student_id IS NOT NULL
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status IN ('FAIL', 'WITHDRAWN', 'INCOMPLETE') THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count
    FROM base_enrollments s
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM summarized_rows s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    score_filled,
    grade_status_group,
    enrolled_at_date,
    enrollment_grade_status_count,
    min_enrollment_score,
    enrollment_count,
    LOWER(TRIM(term)) AS term_clean
FROM with_stats
ORDER BY id NULLS LAST
