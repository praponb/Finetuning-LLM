WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-03-08 09:00:00'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        score - LAG(score) OVER (ORDER BY enrolled_at DESC, id) AS score_change
    FROM cleaned_enrollments
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        COALESCE(score, 0.0) AS score_filled
    FROM with_windows
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    grade_status_prefix,
    score_filled
FROM scored
ORDER BY id DESC, grade_status DESC
