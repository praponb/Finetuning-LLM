WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        MONTH(enrolled_at) AS enrolled_at_month,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
    FROM enrollments
    WHERE (score >= 38.3 OR term IN ('2025T2', '2024T2'))
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-03-14 07:00:00'
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at_month,
        enrolled_at_plus_1d,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        MONTH(enrolled_at) AS enrolled_at_month_2
    FROM with_activity
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_month,
        s.enrolled_at_plus_1d,
        s.enrolled_at_dayofweek,
        s.enrolled_at_month_2,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM labeled s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND MONTH(e.enrolled_at) IN (1, 2)
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at_2
    FROM exploded_detail_enrollments s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        UPPER(TRIM(term)) AS term_clean,
        HOUR(enrollment_enrolled_at) AS enrollment_enrolled_at_hour
    FROM profiled
    WHERE enrollment_id <= 9
)
SELECT id, enrollment_id, field, reading
FROM (
    SELECT id, enrollment_id, enrollment_enrolled_at_hour, enrolled_at_month, enrolled_at_dayofweek
    FROM enriched
) src
UNPIVOT (
    reading FOR field IN (enrollment_enrolled_at_hour AS m_enrollment_enrolled_at_hour, enrolled_at_month AS enrolled_at_month_v, enrolled_at_dayofweek AS m_enrolled_at_dayofweek)
) unp
