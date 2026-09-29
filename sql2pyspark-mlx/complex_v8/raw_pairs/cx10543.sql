WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept
    FROM cleaned_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
grouped AS (
    -- Group-level totals
    SELECT student_id, MIN(score) AS min_score, MAX(enrolled_at) AS last_enrolled_at
    FROM joined_course
    GROUP BY 1
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.student_id,
        s.min_score,
        s.last_enrolled_at,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM grouped s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on >= DATE '2024-04-01'
)
SELECT *
FROM (
    SELECT last_enrolled_at, student_id, student_cohort, min_score
    FROM enriched
) src
PIVOT (
    SUM(min_score)
    FOR student_cohort IN ('C2024', 'C2023')
)
ORDER BY last_enrolled_at, student_id
