WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
      AND score <= 83.1
),
latest_per_key AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM base_enrollments
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM latest_per_key
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.days_since_enrolled_at,
        st.gpa AS student_gpa,
        st.major AS student_major,
        st.student_name
    FROM scored s
    JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(st.student_name)) LIKE 'A%'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM enriched s
),
enriched_2 AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.student_major,
        s.last_enrollment_enrolled_at,
        st.cohort AS student_cohort
    FROM with_stats s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    student_major,
    last_enrollment_enrolled_at
FROM enriched_2
ORDER BY id
LIMIT 10
