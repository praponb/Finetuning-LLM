WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.score) AS prior_score_7d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS enrollment_term_count,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM in_window s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
latest AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_7d,
        prior_score_7d,
        enrollment_term_count,
        enrollment_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY prior_rows_7d DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM latest
)
SELECT
    enrollment_term_count,
    enrollment_count,
    student_id,
    GROUPING_ID() AS grouping_level,
    MIN(enrolled_at) AS first_enrolled_at,
    COUNT(*) AS cnt,
    MAX(enrollment_term_count) AS max_enrollment_term_count
FROM labeled
GROUP BY GROUPING SETS ((enrollment_term_count, enrollment_count, student_id), (enrollment_term_count))
ORDER BY enrollment_term_count NULLS LAST, enrollment_count NULLS LAST, student_id NULLS LAST
