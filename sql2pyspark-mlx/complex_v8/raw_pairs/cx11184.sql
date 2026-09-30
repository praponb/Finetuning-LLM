WITH prepared AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE gpa > 2.5
      AND major <> 'CS'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        e.total_enrollment_score
    FROM prepared s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('YEAR', enrolled_on) AS enrolled_on_year,
        MONTH(enrolled_on) AS enrolled_on_month,
        DATE_FORMAT(enrolled_on, 'MMM yyyy') AS enrolled_on_label
    FROM with_stats
)
SELECT
    id,
    major,
    enrolled_on,
    is_international_label,
    enrollment_count,
    enrollment_count_2,
    total_enrollment_score,
    enrolled_on_month,
    LOWER(TRIM(is_international_label)) AS is_international_label_clean
FROM scored
ORDER BY is_international_label DESC, enrolled_on NULLS LAST
