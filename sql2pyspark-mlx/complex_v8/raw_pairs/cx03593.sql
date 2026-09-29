WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        CASE WHEN score >= 57.8 THEN 'large' WHEN score >= 39.4 THEN 'medium' ELSE 'small' END AS score_tier,
        CASE WHEN score >= 71.0 THEN 'large' WHEN score >= 34.2 THEN 'medium' ELSE 'small' END AS score_tier_2
    FROM enrollments
    WHERE (course_id IS NOT NULL OR MONTH(enrolled_at) IN (1, 3))
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.score_tier,
        s.score_tier_2,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.course_id
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.score_tier_2,
        s.enrollment_id,
        s.enrollment_score,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score_2,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status_2
    FROM line_level s
    JOIN enrollments e ON e.student_id = s.student_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        score_tier_2,
        enrollment_id,
        enrollment_score,
        enrollment_id_2,
        enrollment_term,
        enrollment_course_id,
        enrollment_grade_status_2,
        CONCAT(enrollment_term, '#', CAST(enrollment_id AS STRING)) AS enrollment_term_ref
    FROM line_level_enrollments
    WHERE enrollment_term NOT IN ('2024T2', '2025T1')
),
with_parent_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrollment_id,
        s.enrollment_id_2,
        s.enrollment_term,
        s.enrollment_course_id,
        s.enrollment_grade_status_2,
        s.enrollment_term_ref,
        c.level AS course_level,
        c.course_code,
        c.credits AS course_credits
    FROM tagged s
    JOIN courses c ON c.id = s.enrollment_course_id
    WHERE c.course_code IS NOT NULL
)
SELECT
    id,
    student_id,
    course_id,
    enrollment_id,
    enrollment_id_2,
    enrollment_term,
    enrollment_course_id,
    course_level,
    course_code,
    COALESCE(enrollment_term, 'UNKNOWN') AS enrollment_term_or_unknown,
    CONCAT(enrollment_term, '#', CAST(course_id AS STRING)) AS enrollment_term_ref_2
FROM with_parent_course
ORDER BY course_id DESC, enrollment_term_or_unknown NULLS LAST
