WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        CASE WHEN score >= 81.4 THEN 'HIGH' WHEN score >= 42.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE (student_id <= 19 OR course_id <= 3)
),
with_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_slug,
        s.score_tier,
        st.is_international AS student_is_international
    FROM cleaned_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        enrolled_at,
        score_tier,
        student_is_international,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_term_student_is_internationa)) / 60, 1) AS minutes_since_prev_enrolled_at,
        RANK() OVER w_term_student_is_internationa AS enrolled_at_rank
    FROM with_details
    WINDOW w_term_student_is_internationa AS (PARTITION BY term, student_is_international ORDER BY enrolled_at DESC NULLS FIRST, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, score_tier, minutes_since_prev_enrolled_at
    FROM ranked
) src
PIVOT (
    AVG(minutes_since_prev_enrolled_at)
    FOR score_tier IN ('LOW', 'MEDIUM')
)
ORDER BY enrolled_at
