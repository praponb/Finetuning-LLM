WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        student_id,
        term,
        MAX(score) AS max_score,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        COUNT(*) FILTER (WHERE grade_status <> 'WITHDRAWN') AS n_grade_status_withdrawn
    FROM base
    WHERE score > 8.7
    GROUP BY course_id, student_id, term
    HAVING MAX(score) > 26.2
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.course_id,
        s.student_id,
        s.term,
        s.max_score,
        s.grade_status_list,
        s.n_grade_status_withdrawn,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM summary s
    JOIN students st ON st.id = s.student_id
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.course_id >= 3
)
SELECT course_id, student_id, measure, amount
FROM (
    SELECT course_id, student_id, student_gpa, max_score
    FROM filtered
) src
UNPIVOT (
    amount FOR measure IN (student_gpa, max_score)
) unp
