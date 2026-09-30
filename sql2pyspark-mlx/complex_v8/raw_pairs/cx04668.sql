WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label,
        LOWER(TRIM(grade_status)) AS grade_status_clean,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE score <= 90.9
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_clean,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM recent s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        grade_status_clean,
        enrollment_id,
        enrollment_student_id,
        enrollment_grade_status,
        enrollment_term,
        ROW_NUMBER() OVER (PARTITION BY enrollment_term ORDER BY enrolled_at, id, enrollment_id) AS enrolled_at_seq,
        DENSE_RANK() OVER (PARTITION BY enrollment_term ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER w_enrollment_term_by_enrolled, 3) AS enrolled_at_cume_dist,
        ROUND(CUME_DIST() OVER w_enrollment_term_by_enrolled_2, 3) AS enrolled_at_cume_dist_2
    FROM exploded_detail
    WINDOW w_enrollment_term_by_enrolled AS (PARTITION BY enrollment_term ORDER BY enrolled_at, id, enrollment_id), w_enrollment_term_by_enrolled_2 AS (PARTITION BY enrollment_term ORDER BY enrolled_at)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) FILTER (WHERE grade_status <> 'WITHDRAWN') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM with_windows s
    LEFT JOIN enrollment_stats e ON e.student_id = s.enrollment_student_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, enrollment_id, metric, reading
FROM (
    SELECT id, enrollment_id, enrolled_at_cume_dist, enrolled_at_cume_dist_2
    FROM profiled
) src
UNPIVOT (
    reading FOR metric IN (enrolled_at_cume_dist AS m_enrolled_at_cume_dist, enrolled_at_cume_dist_2 AS m_enrolled_at_cume_dist_2)
) unp
