WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE score > 38.1
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score
    FROM base s
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.avg_enrollment_score,
        st.major AS student_major,
        st.is_international AS student_is_international,
        c.credits AS course_credits,
        c.course_code
    FROM with_counts s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        course_credits - avg_enrollment_score AS course_credits_minus_avg_enrollment_score
    FROM enriched_student
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', course_code, student_major) AS course_code_student_major_label
    FROM tagged
    WHERE course_credits BETWEEN 2 AND 3
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FILTER (WHERE e.term = '2024T2') FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count
    FROM scored s
)
SELECT
    enrollment_term_count,
    student_is_international,
    course_credits,
    MIN(course_credits) AS min_course_credits
FROM profiled
GROUP BY GROUPING SETS ((enrollment_term_count, student_is_international, course_credits), (enrollment_term_count), ())
