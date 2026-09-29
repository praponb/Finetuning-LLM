WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND term <> '2025T1'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(score, 0.0) AS score_filled,
        YEAR(enrolled_at) AS enrolled_at_year,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM recent
    WHERE student_id <= 9
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_year,
        enrolled_at_quarter,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix,
        ROUND(score_filled / NULLIF(enrolled_at_year, 0), 3) AS score_filled_per_enrolled_at_year
    FROM scored
    WHERE course_id IS NOT NULL
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status = 'PASS'
        )
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    enrolled_at_year,
    enrolled_at_quarter,
    enrolled_at_plus_1d,
    term_prefix,
    ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(enrolled_at_quarter)) / 60, 1) AS minutes_enrolled_at_quarter_to_enrolled_at
FROM screened
WHERE term_prefix IS NOT NULL
