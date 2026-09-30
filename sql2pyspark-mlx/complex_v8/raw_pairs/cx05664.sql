WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (2, 3)
      AND student_id <= 14
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM recent s
),
latest AS (
    -- Keep the earliest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_clean,
        first_enrollment_enrolled_at,
        enrollment_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY score, id) AS row_num
        FROM profiled
    ) ranked
    WHERE row_num = 1
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.first_enrollment_enrolled_at,
        s.enrollment_count,
        c.credits AS course_credits
    FROM latest s
    JOIN courses c ON c.id = s.course_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM combined_details_course
    WHERE HOUR(first_enrollment_enrolled_at) BETWEEN 7 AND 12
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM labeled s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.score BETWEEN 13.9 AND 66.0
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(enrollment_count, 0) AS enrollment_count_filled,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT_WS('-', grade_status_slug, grade_status) AS grade_status_slug_grade_status_label
    FROM filtered
)
SELECT
    grade_status_or_unknown,
    grade_status,
    enrollment_count_filled,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    ROUND(SUM(course_credits) / NULLIF(SUM(enrollment_count_filled), 0), 3) AS course_credits_to_enrollment_count_filled_ratio
FROM scored
GROUP BY GROUPING SETS ((grade_status_or_unknown, grade_status, enrollment_count_filled), (grade_status_or_unknown), ())
ORDER BY grade_status_or_unknown NULLS LAST, grade_status NULLS LAST, enrollment_count_filled NULLS LAST
