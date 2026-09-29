WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        ROUND(score * 1.5, 2) AS score_adj,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
    WHERE course_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        SUM(score) AS total_enrollment_score,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.total_enrollment_score,
        e.min_enrollment_score
    FROM cleaned s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
enrollment_stats_2 AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) FILTER (WHERE grade_status IN ('FAIL', 'WITHDRAWN')) AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM with_counts s
    LEFT JOIN enrollment_stats_2 e ON e.student_id = s.student_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(total_enrollment_score - 300.0) AS total_enrollment_score_distance,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM summarized_rows
    WHERE score >= 38.3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (ORDER BY total_enrollment_score, id) AS total_enrollment_score_quartile,
        ROUND(AVG(score) OVER (ORDER BY total_enrollment_score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_avg_4,
        MAX(enrollment_count) OVER (ORDER BY total_enrollment_score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS enrollment_count_max_4,
        MIN(min_enrollment_score) OVER (PARTITION BY term, term_len) AS term_term_len_min_min_enrollment_score,
        NTH_VALUE(enrolled_at_plus_30m, 2) OVER (PARTITION BY student_id ORDER BY enrollment_grade_status_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_30m
    FROM enriched
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-02-12 12:00:00'
        )
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    term_len,
    score_adj,
    enrolled_at_plus_30m,
    enrollment_count,
    total_enrollment_score,
    min_enrollment_score,
    enrollment_grade_status_count,
    total_enrollment_score_distance,
    grade_status_group,
    term_term_len_min_min_enrollment_score,
    second_enrolled_at_plus_30m,
    SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
FROM with_activity
ORDER BY course_id DESC NULLS FIRST
