WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 63.9) AS score_distance,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE (score > 16.3 OR TO_DATE(enrolled_at) < DATE '2025-03-14')
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_distance,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa,
        c.credits AS course_credits,
        c.level AS course_level
    FROM base s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        score_distance,
        student_gpa,
        course_level,
        course_credits - LAG(course_credits) OVER w_all_by_student_gpa AS course_credits_change,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(AVG(score_distance) OVER (ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_distance_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY student_gpa DESC, id), 3) AS student_gpa_percent_rank,
        SUM(student_gpa) OVER (PARTITION BY student_id, grade_status ORDER BY student_gpa DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS student_gpa_sum_4,
        LAST_VALUE(course_credits) OVER (ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits,
        FIRST_VALUE(course_credits) OVER (ORDER BY score_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        ROUND(CUME_DIST() OVER w_all_by_student_gpa_ties, 3) AS student_gpa_cume_dist,
        ROUND(PERCENT_RANK() OVER w_course_id_student_id_by_scor, 3) AS score_distance_percent_rank
    FROM with_details_student
    WINDOW w_all_by_student_gpa AS (ORDER BY student_gpa NULLS LAST, id), w_all_by_student_gpa_ties AS (ORDER BY student_gpa NULLS LAST), w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score_distance DESC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(score * 0.9, 2) AS score_adj,
        SUBSTRING(TRIM(course_level), 1, 3) AS course_level_prefix,
        score - student_gpa AS score_minus_student_gpa
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    score_distance,
    student_gpa,
    course_level,
    course_credits_change,
    rows_in_course_id,
    student_gpa_percent_rank,
    last_course_credits,
    first_course_credits,
    score_distance_percent_rank,
    score_adj,
    course_level_prefix,
    score_minus_student_gpa,
    ROUND(student_gpa_sum_4 / NULLIF(score_distance, 0), 3) AS student_gpa_sum_4_per_score_distance
FROM labeled
ORDER BY rows_in_course_id NULLS LAST, course_level DESC NULLS FIRST
