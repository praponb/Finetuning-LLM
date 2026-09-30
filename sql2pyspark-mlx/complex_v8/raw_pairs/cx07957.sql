WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score >= 16.3
),
filtered_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE score < (SELECT MAX(score) FROM filtered)
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.course_code,
        st.student_name,
        st.gpa AS student_gpa
    FROM filtered_2 s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.course_code,
        s.student_name,
        s.student_gpa,
        c.course_code AS course_course_code,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM with_parent_course s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept = 'Bio'
),
derived AS (
    -- Derive helper columns
    SELECT *, ABS(student_gpa - 2.9) AS student_gpa_distance
    FROM with_details_course
    WHERE student_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        score,
        student_name,
        course_credits,
        COUNT(*) OVER (PARTITION BY course_dept, student_id) AS rows_in_course_dept_student_id,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY course_dept, student_id)) AS distinct_course_id_in_course_dept_student_id,
        MIN(score) OVER (PARTITION BY course_credits ORDER BY student_gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        LEAD(course_credits) OVER w_course_credits_by_student_gp_2 AS next_course_credits
    FROM derived
    WINDOW w_course_credits_by_student_gp_2 AS (PARTITION BY course_credits ORDER BY student_gpa DESC, id)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.course_id,
        s.score,
        s.student_name,
        s.course_credits,
        s.rows_in_course_dept_student_id,
        s.distinct_course_id_in_course_dept_student_id,
        s.next_course_credits,
        c.level AS course_level
    FROM sequenced s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    rows_in_course_dept_student_id,
    next_course_credits,
    course_level
FROM joined
ORDER BY next_course_credits NULLS LAST, rows_in_course_dept_student_id DESC NULLS FIRST, id
LIMIT 20
