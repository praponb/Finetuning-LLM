WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        CASE WHEN score >= 83.1 THEN 'A' WHEN score >= 34.2 THEN 'B' ELSE 'C' END AS score_tier,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        score_adj,
        grade_status_clean,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        COALESCE(score, 0.0) AS score_filled,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM recent_enrollments
    WHERE UPPER(TRIM(grade_status_clean)) LIKE 'F%'
),
filtered AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.level IN ('advanced', 'core')
              AND c.credits > 2
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(score / NULLIF(enrolled_at_year, 0), 3) AS score_per_enrolled_at_year
    FROM filtered
    WHERE score_adj > 22.5
)
SELECT
    id,
    student_id,
    course_id,
    score,
    score_adj,
    grade_status_clean,
    enrolled_at_label,
    score_filled,
    enrolled_at_year,
    ROUND(score * 1.07, 2) AS score_adj_2,
    LOWER(TRIM(enrolled_at_label)) AS enrolled_at_label_clean
FROM tagged
ORDER BY student_id DESC
