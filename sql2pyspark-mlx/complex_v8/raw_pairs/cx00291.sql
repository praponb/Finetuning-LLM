WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE score > 6.2
      AND student_id IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(score, 0.0) AS score_filled
    FROM filtered
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        s.enrolled_at_day,
        s.grade_status_prefix,
        s.score_filled,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM labeled s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level IS NOT NULL
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    days_since_enrolled_at,
    grade_status_prefix,
    score_filled,
    course_credits,
    course_level,
    course_dept
FROM enriched
WHERE course_dept = 'Biz'
ORDER BY score
