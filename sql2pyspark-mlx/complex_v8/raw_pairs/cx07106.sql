WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
      AND student_id <= 15
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_date,
        st.major AS student_major,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(student_gpa) OVER (ORDER BY student_gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_student_gpa,
        MIN(score) OVER (PARTITION BY grade_status, student_major ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_dense_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id,
        MAX(score) OVER (PARTITION BY grade_status, student_major ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_max_1_prior,
        RANK() OVER (ORDER BY student_gpa DESC) AS student_gpa_rank,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status, student_major ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        LEAD(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at NULLS LAST, id) AS next_enrolled_at,
        MIN(score) OVER (PARTITION BY student_major, student_cohort) AS student_major_student_cohort_min_score
    FROM with_details_student
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_date,
        student_major,
        student_gpa,
        student_cohort,
        running_student_gpa,
        min_score_so_far,
        enrolled_at_dense_rank,
        distinct_term_in_student_id,
        score_max_1_prior,
        last_known_score,
        next_enrolled_at,
        student_major_student_cohort_min_score
    FROM metrics
    WHERE student_gpa_rank <= 3
      AND running_student_gpa > 7.3
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    student_major,
    student_gpa,
    running_student_gpa,
    min_score_so_far,
    enrolled_at_dense_rank,
    distinct_term_in_student_id,
    score_max_1_prior,
    next_enrolled_at
FROM first_rows
WHERE score_max_1_prior >= 90.0
ORDER BY score_max_1_prior NULLS LAST, grade_status
