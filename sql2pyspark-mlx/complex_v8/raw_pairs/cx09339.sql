WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE student_id <= 8
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(grade_status_group)) AS grade_status_group_clean,
        FLOOR(score_filled / 25) * 25 AS score_filled_bucket
    FROM prepared_enrollments
    WHERE grade_status <> 'FAIL'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at
    FROM tagged s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id IS NOT NULL
        )
      AND s.student_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        enrolled_at,
        grade_status_prefix,
        score_filled,
        grade_status_group,
        enrollment_count,
        ROUND(AVG(score_filled_bucket) OVER w_course_id, 2) AS course_id_avg_score_filled_bucket,
        ROUND(score_filled_bucket / NULLIF(SUM(score_filled_bucket) OVER (PARTITION BY course_id), 0), 4) AS score_filled_bucket_share_of_course_id,
        ROUND(CUME_DIST() OVER (ORDER BY enrollment_count ASC), 3) AS enrollment_count_cume_dist,
        LAG(score_filled, 1, 0) OVER (ORDER BY enrollment_count ASC, id) AS prev_score_filled,
        SIZE(COLLECT_SET(grade_status_group) OVER (PARTITION BY course_id)) AS distinct_grade_status_group_in_course_id
    FROM qualified
    WINDOW w_course_id AS (PARTITION BY course_id)
)
-- Final projection
SELECT
    id,
    enrolled_at,
    grade_status_prefix,
    score_filled,
    grade_status_group,
    course_id_avg_score_filled_bucket,
    score_filled_bucket_share_of_course_id,
    enrollment_count_cume_dist,
    prev_score_filled
FROM ranked
