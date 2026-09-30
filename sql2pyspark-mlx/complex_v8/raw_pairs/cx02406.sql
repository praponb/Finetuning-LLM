WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.course_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        enrollment_id,
        enrollment_student_id,
        enrollment_term,
        enrollment_score,
        enrollment_grade_status,
        LENGTH(TRIM(enrollment_term)) AS enrollment_term_len,
        CONCAT(enrollment_term, '#', CAST(enrollment_id AS STRING)) AS enrollment_term_ref,
        FLOOR(enrollment_score / 25) * 25 AS enrollment_score_bucket
    FROM with_children_enrollments
    WHERE course_id IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per enrollment_term, enrollment_student_id
    SELECT
        id,
        student_id,
        enrollment_id,
        enrollment_student_id,
        enrollment_term,
        enrollment_score,
        enrollment_grade_status,
        enrollment_term_len,
        enrollment_term_ref,
        enrollment_score_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY enrollment_term, enrollment_student_id ORDER BY enrollment_score, id, enrollment_id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_term,
    student_id,
    MIN(enrollment_term_len) AS min_enrollment_term_len,
    COUNT(*) AS n_rows
FROM deduped
GROUP BY GROUPING SETS ((enrollment_term, student_id), (enrollment_term), (student_id), ())
ORDER BY enrollment_term NULLS LAST, student_id NULLS LAST
