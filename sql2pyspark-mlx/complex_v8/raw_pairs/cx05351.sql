WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE score >= 11.3
      AND course_id IS NOT NULL
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score
    FROM valid s
),
filtered AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.level NOT IN ('core', 'intro')
              AND c.credits >= 2
        )
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id <= 10
        )
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(avg_enrollment_score / 25) * 25 AS avg_enrollment_score_bucket,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM qualified
    WHERE course_id >= 5
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    term_prefix,
    avg_enrollment_score,
    term_grade_status_label,
    FLOOR(term_len / 2) * 2 AS term_len_bucket,
    ROUND(avg_enrollment_score * 1.5, 2) AS avg_enrollment_score_adj
FROM scored
ORDER BY id DESC
