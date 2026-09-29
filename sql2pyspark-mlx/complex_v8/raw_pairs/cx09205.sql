WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        term_clean,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM base
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term_clean,
        grade_status_or_unknown,
        grade_status_ref,
        grade_status_clean,
        SUBSTRING(TRIM(grade_status_ref), 1, 3) AS grade_status_ref_prefix,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        LOWER(REGEXP_REPLACE(TRIM(grade_status_clean), ' ', '_')) AS grade_status_clean_slug
    FROM derived
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_or_unknown,
        s.grade_status_ref_prefix,
        s.grade_status_clean_slug,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM labeled s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
exploded_detail_2 AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_or_unknown,
        s.grade_status_ref_prefix,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_enrolled_at,
        e.id AS enrollment_id_2,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM exploded_detail s
    JOIN enrollments e ON e.student_id = s.student_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', enrollment_enrolled_at_2) AS enrollment_enrolled_at_2_month,
        COUNT(*) FILTER (WHERE enrollment_term NOT IN ('2024T1', '2024T2')) AS n_enrollment_term_not_in_2024t,
        COUNT(*) AS n_rows,
        MIN(enrollment_enrolled_at_2) AS first_enrollment_enrolled_at_2,
        ROUND(100 * COUNT(CASE WHEN enrollment_term NOT IN ('2024T1', '2025T1') THEN 1 END) / COUNT(*), 1) AS pct_enrollment_term_not_in_2024t,
        MAX(enrollment_enrolled_at_2) AS last_enrollment_enrolled_at_2
    FROM exploded_detail_2
    GROUP BY DATE_TRUNC('MONTH', enrollment_enrolled_at_2)
    HAVING COUNT(*) >= 3
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE n_rows > (SELECT MIN(n_rows) FROM summary)
)
-- Final projection
SELECT
    enrollment_enrolled_at_2_month,
    n_enrollment_term_not_in_2024t,
    first_enrollment_enrolled_at_2,
    last_enrollment_enrolled_at_2,
    CASE WHEN n_rows >= 10 THEN 'A' WHEN n_rows >= 4 THEN 'B' ELSE 'C' END AS n_rows_tier
FROM without_matches
WHERE enrollment_enrolled_at_2_month >= TIMESTAMP '2025-01-13 09:00:00'
ORDER BY n_rows_tier DESC, enrollment_enrolled_at_2_month
LIMIT 20
