WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa
    FROM valid s
    JOIN students st ON st.id = s.student_id
),
tagged AS (
    -- Derive helper columns
    SELECT *, ABS(score - 49.0) AS score_distance
    FROM combined_details
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(grade_status)) AS grade_status_clean,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM tagged
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_ADD(student_enrolled_on, 90) AS student_enrolled_on_plus_90d
    FROM enriched
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM scored s
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    student_is_international,
    student_enrolled_on,
    student_gpa,
    grade_status_clean,
    term_prefix,
    student_enrolled_on_plus_90d,
    enrollment_count,
    max_enrollment_score,
    first_enrollment_enrolled_at
FROM summarized_rows
ORDER BY first_enrollment_enrolled_at DESC, id
LIMIT 5
