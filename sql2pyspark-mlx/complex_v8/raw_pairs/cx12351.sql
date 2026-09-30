WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at_week,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM cleaned
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at_week,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM tagged s
    JOIN enrollments e ON e.student_id = s.student_id
),
latest_per_key AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at_week,
        enrollment_id,
        enrollment_score,
        enrollment_enrolled_at,
        enrollment_term
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrollment_score DESC, id DESC, enrollment_id DESC) AS row_num
        FROM detail_rows_enrollments
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Join enrollments
    SELECT s.*, e.grade_status AS enrollment_grade_status
    FROM latest_per_key s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
    WHERE e.grade_status <> 'FAIL'
),
without_matches AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND LOWER(st.student_name) LIKE '%ch%'
        )
)
SELECT
    id,
    student_id,
    course_id,
    score,
    enrolled_at_week,
    enrollment_id,
    enrollment_score,
    enrollment_enrolled_at,
    enrollment_term,
    enrollment_grade_status,
    ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(enrolled_at_week)) / 3600, 1) AS hours_enrolled_at_week_to_enrollment_enrolled_at,
    score + enrollment_score AS score_plus_enrollment_score
FROM without_matches
ORDER BY enrolled_at_week, id, enrollment_id
LIMIT 20
