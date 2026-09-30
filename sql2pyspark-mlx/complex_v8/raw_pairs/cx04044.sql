WITH recent AS (
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
    WHERE student_id IS NOT NULL
      AND grade_status = 'INCOMPLETE'
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.student_name
    FROM recent s
    JOIN students st ON st.id = s.student_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM joined_student s
    JOIN enrollments e ON e.course_id = s.course_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(enrollment_score, 0.0) AS enrollment_score_filled,
        FLOOR(enrollment_score / 25) * 25 AS enrollment_score_bucket,
        INITCAP(TRIM(term)) AS term_clean
    FROM line_level
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        enrollment_score_filled + enrollment_score AS enrollment_score_filled_plus_enrollment_score,
        CONCAT_WS(' / ', term_clean, term) AS term_clean_term_label
    FROM derived
    WHERE term NOT IN ('2024T2', '2025T1')
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at_2
    FROM tagged s
)
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    enrollment_id,
    enrollment_grade_status,
    enrollment_student_id,
    enrollment_score_filled,
    enrollment_score_bucket,
    term_clean,
    enrolled_at_date,
    min_enrollment_score,
    first_enrollment_enrolled_at
FROM profiled
WHERE enrolled_at_date >= DATE '2024-08-01'
