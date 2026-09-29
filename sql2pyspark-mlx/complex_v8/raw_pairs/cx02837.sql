WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE score > 8.7
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CASE WHEN grade_status IN ('INCOMPLETE', 'PASS') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM filtered_enrollments
    WHERE grade_status = 'WITHDRAWN'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.student_name,
        c.credits AS course_credits,
        c.course_code
    FROM scored s
    JOIN students st ON st.id = s.student_id AND NOT st.is_international
    JOIN courses c ON c.id = s.course_id
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_prefix,
        s.score_bucket,
        s.term_or_unknown,
        s.grade_status_group,
        s.enrolled_at_label,
        s.student_name,
        s.course_credits,
        s.course_code,
        st.student_name AS student_student_name
    FROM joined s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    term_or_unknown,
    grade_status_group,
    enrolled_at_label,
    student_name,
    course_credits,
    course_code,
    student_student_name,
    DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label_2
FROM with_details_student
WHERE grade_status_group NOT IN ('group_a', 'group_b')
ORDER BY enrolled_at_label_2 DESC
