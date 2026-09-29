WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE score > 43.5
      AND HOUR(enrolled_at) BETWEEN 7 AND 9
),
latest AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        days_since_enrolled_at,
        grade_status_len,
        grade_status_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY days_since_enrolled_at DESC, id DESC) AS row_num
        FROM filtered_enrollments
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
course_id_stats AS (
    -- Median grade_status_len per course_id
    SELECT course_id, PERCENTILE_APPROX(grade_status_len, 0.5) AS median_grade_status_len
    FROM screened
    GROUP BY course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_grade_status_len,
        ROUND(s.grade_status_len - g.median_grade_status_len, 2) AS grade_status_len_minus_median
    FROM screened s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.grade_status_len >= g.median_grade_status_len
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        s.grade_status_slug,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM compared s
    JOIN enrollments e ON e.course_id = s.course_id
),
screened_2 AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_children_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
              AND e.student_id IS NOT NULL
        )
),
latest_per_key AS (
    -- Keep the latest row per student_id, enrollment_term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        days_since_enrolled_at,
        grade_status_slug,
        enrollment_id,
        enrollment_grade_status,
        enrollment_term,
        enrollment_score,
        enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, enrollment_term ORDER BY enrollment_enrolled_at DESC, id DESC, enrollment_id DESC) AS row_num
        FROM screened_2
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    student_id,
    term,
    score,
    days_since_enrolled_at,
    enrollment_id,
    enrollment_grade_status,
    enrollment_score
FROM latest_per_key
ORDER BY term
