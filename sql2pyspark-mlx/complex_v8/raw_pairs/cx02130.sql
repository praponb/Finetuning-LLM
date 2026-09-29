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
        CASE WHEN score >= 57.0 THEN 'large' WHEN score >= 43.5 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id <= 2
)
SELECT term, enrolled_at AS event_time, score_tier
FROM base
WHERE term = '2024T1'
UNION ALL
SELECT term, enrolled_at AS event_time, grade_status AS score_tier
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-01-31 14:00:00'
UNION ALL
SELECT grade_status AS term, enrolled_at AS event_time, term AS score_tier
FROM enrollments
