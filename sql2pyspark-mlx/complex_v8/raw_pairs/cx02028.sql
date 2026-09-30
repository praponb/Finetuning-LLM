WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 06:00:00'
      AND student_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per course_id, grade_status
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        days_since_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM valid_enrollments
    ) ranked
    WHERE row_num = 1
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM unique_rows s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at < TIMESTAMP '2025-02-18 09:00:00'
),
unique_rows_2 AS (
    -- Keep the earliest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        enrollment_id,
        enrollment_grade_status,
        enrollment_term,
        enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrollment_enrolled_at, id, enrollment_id) AS recency_rank
        FROM exploded_detail
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    course_id,
    grade_status,
    enrolled_at,
    enrollment_id,
    enrollment_grade_status,
    enrollment_term,
    enrollment_enrolled_at
FROM unique_rows_2
WHERE enrollment_term <> '2025T1'
ORDER BY grade_status DESC
