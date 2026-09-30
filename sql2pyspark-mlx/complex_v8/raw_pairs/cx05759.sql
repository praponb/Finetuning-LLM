WITH cleaned_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE (credits >= 2 OR dept IS NOT NULL)
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.credits,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM cleaned_courses s
    JOIN enrollments e ON e.course_id = s.id AND e.grade_status IN ('INCOMPLETE', 'FAIL', 'WITHDRAWN')
),
with_parent_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.course_code,
        s.credits,
        s.enrollment_id,
        s.enrollment_grade_status,
        e.course_id AS enrollment_course_id,
        st.is_international AS student_is_international
    FROM exploded_detail s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = e.student_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(credits - 3) AS credits_distance,
        COALESCE(credits, 0) AS credits_filled,
        SUBSTRING(TRIM(enrollment_grade_status), 1, 2) AS enrollment_grade_status_prefix
    FROM with_parent_enrollment
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(credits_filled / NULLIF(credits_distance, 0), 3) AS credits_filled_per_credits_distance
    FROM screened
    WHERE UPPER(TRIM(course_code)) LIKE 'A%'
)
-- Final projection
SELECT
    id,
    course_code,
    enrollment_grade_status,
    enrollment_course_id,
    student_is_international,
    credits_distance,
    enrollment_grade_status_prefix,
    credits_filled_per_credits_distance
FROM derived_2
WHERE enrollment_course_id >= 7
ORDER BY credits_distance DESC
