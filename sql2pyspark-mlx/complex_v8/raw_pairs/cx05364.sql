WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        LENGTH(TRIM(term)) AS term_len,
        LENGTH(TRIM(grade_status)) AS grade_status_len_2
    FROM enrollments
    WHERE score < 58.8
      AND student_id >= 7
),
labeled AS (
    -- Add calculated columns
    SELECT *, MONTH(enrolled_at) AS enrolled_at_month
    FROM filtered_enrollments
)
SELECT *
FROM (
    SELECT grade_status_len, enrolled_at_month, grade_status, term_len
    FROM labeled
) src
PIVOT (
    SUM(term_len)
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS', 'FAIL')
)
