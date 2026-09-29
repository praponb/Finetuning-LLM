WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE grade_status = 'PASS'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(score, 0.0) AS score_filled,
        CONCAT_WS(' / ', term_grade_status_label, grade_status) AS term_grade_status_label_grade_status_label,
        LOWER(REGEXP_REPLACE(TRIM(term_grade_status_label), ' ', '_')) AS term_grade_status_label_slug
    FROM valid
)
SELECT
    id,
    term,
    score,
    grade_status,
    score_filled,
    term_grade_status_label_slug
FROM annotated
ORDER BY grade_status, id DESC
