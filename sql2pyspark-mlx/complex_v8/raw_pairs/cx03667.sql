WITH recent AS (
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
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 15:00:00'
      AND score <= 74.9
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY course_credits ORDER BY enrolled_at, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_credits ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        ROUND(AVG(score) OVER (PARTITION BY course_credits, DATE_TRUNC('MONTH', enrolled_at)), 2) AS course_credits_enrolled_at_month_avg_score
    FROM joined
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.course_credits,
        s.course_dept,
        s.rn,
        s.course_credits_enrolled_at_month_avg_score,
        c.level AS course_level,
        st.student_name,
        st.is_international AS student_is_international
    FROM metrics s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.course_dept,
        s.rn,
        s.course_level,
        s.student_is_international,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM joined_course s
    JOIN enrollments e ON e.course_id = s.course_id AND e.term IN ('2024T2', '2025T1', '2025T2')
),
enriched AS (
    -- Join students
    SELECT s.*, st.cohort AS student_cohort
    FROM line_level_enrollments s
    JOIN students st ON st.id = s.student_id AND st.major <> 'Math'
)
SELECT
    id,
    course_id,
    term,
    enrolled_at,
    course_dept,
    rn,
    course_level,
    student_is_international,
    enrollment_id,
    enrollment_term,
    enrollment_enrolled_at,
    student_cohort
FROM enriched
WHERE student_cohort = 'C2023'
ORDER BY enrollment_enrolled_at
