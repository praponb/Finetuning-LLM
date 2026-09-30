WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND term IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(term)) AS term_clean,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        ROUND(score / NULLIF(score_bucket, 0), 3) AS score_per_score_bucket
    FROM recent_enrollments
    WHERE grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
),
summary AS (
    -- Summarise per group
    SELECT
        student_id,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        grade_status,
        COUNT(*) AS n_rows,
        SUM(score_bucket) AS total_score_bucket,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(enrolled_at) AS first_enrolled_at
    FROM annotated
    WHERE score_per_score_bucket < 7.0
    GROUP BY student_id, DATE_TRUNC('MONTH', enrolled_at), grade_status
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score
    FROM summary s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.enrolled_at_month,
        s.grade_status,
        s.n_rows,
        s.total_score_bucket,
        s.distinct_ids,
        s.first_enrolled_at,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM profiled s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
without_matches AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM with_children_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.enrollment_course_id
              AND c.credits < 4
        )
)
-- Final projection
SELECT
    student_id,
    enrolled_at_month,
    n_rows,
    total_score_bucket,
    distinct_ids,
    first_enrolled_at,
    enrollment_enrolled_at,
    enrollment_course_id,
    CASE WHEN total_score_bucket >= 480 THEN 'A' WHEN total_score_bucket >= 150 THEN 'B' ELSE 'C' END AS total_score_bucket_tier
FROM without_matches
ORDER BY total_score_bucket_tier
