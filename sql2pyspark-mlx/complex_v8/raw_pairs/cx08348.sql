WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
      AND student_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM deduped
    ) ranked
    WHERE recency_rank = 1
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.student_name,
        st.gpa AS student_gpa,
        st.major AS student_major,
        c.course_code
    FROM unique_rows s
    JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
    LEFT JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        ROUND(AVG(student_gpa) OVER w_term, 2) AS term_avg_student_gpa,
        SUM(student_gpa) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS student_gpa_sum_2
    FROM with_details_student
    WINDOW w_term AS (PARTITION BY term)
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.student_name,
        s.student_major,
        s.course_code,
        s.rows_in_term,
        s.enrolled_at_percent_rank,
        s.term_avg_student_gpa,
        c.dept AS course_dept,
        c.course_code AS course_course_code
    FROM windowed s
    JOIN courses c ON c.id = s.course_id
)
SELECT id, measure, amount
FROM (
    SELECT id, term_avg_student_gpa, enrolled_at_percent_rank, score
    FROM enriched_course
) src
UNPIVOT (
    amount FOR measure IN (term_avg_student_gpa, enrolled_at_percent_rank, score)
) unp
ORDER BY id, measure
