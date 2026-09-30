WITH valid AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major <> 'Biz'
      AND UPPER(TRIM(student_name)) LIKE 'B%'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM valid
    WHERE gpa > (SELECT MIN(gpa) FROM valid)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM screened s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
)
SELECT
    id,
    major,
    enrolled_on,
    gpa,
    is_international,
    enrollment_count,
    COALESCE(major, 'UNKNOWN') AS major_or_unknown
FROM summarized_rows
