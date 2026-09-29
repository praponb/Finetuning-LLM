WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (grade_status NOT IN ('FAIL', 'INCOMPLETE') OR course_id IS NOT NULL)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled,
        FLOOR(score / 25) * 25 AS score_bucket,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM filtered
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_filled,
        s.score_bucket,
        s.days_since_enrolled_at,
        st.major AS student_major
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
              AND e.term = '2025T2'
        )
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score_bucket,
        s.days_since_enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM screened s
    JOIN enrollments e ON e.student_id = s.student_id
),
latest_per_key AS (
    -- Keep the earliest row per student_id, enrollment_term
    SELECT
        id,
        student_id,
        course_id,
        score_bucket,
        days_since_enrolled_at,
        enrollment_id,
        enrollment_term,
        enrollment_enrolled_at,
        enrollment_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, enrollment_term ORDER BY enrollment_score, id, enrollment_id) AS recency_rank
        FROM line_level_enrollments
    ) ranked
    WHERE recency_rank = 1
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score_bucket,
        s.days_since_enrolled_at,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_score,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM latest_per_key s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa <= 3.4
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score_bucket,
    days_since_enrolled_at,
    enrollment_id,
    enrollment_term,
    enrollment_score,
    student_gpa,
    LOWER(REGEXP_REPLACE(TRIM(enrollment_term), ' ', '_')) AS enrollment_term_slug
FROM enriched_student
WHERE student_id IS NOT NULL
ORDER BY enrollment_term, enrollment_id DESC
