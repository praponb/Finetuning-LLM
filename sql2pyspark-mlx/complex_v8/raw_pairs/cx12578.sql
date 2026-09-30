WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE grade_status IN ('PASS', 'FAIL')
),
with_parent_student AS (
    -- Join students
    SELECT s.*, st.enrolled_on AS student_enrolled_on, st.cohort AS student_cohort
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY MONTH(enrolled_at), course_id), 0), 4) AS score_share_of_enrolled_at_month_course_id,
        MIN(score) OVER w_enrolled_at_month_course_id AS enrolled_at_month_course_id_min_score,
        ROUND(CUME_DIST() OVER (ORDER BY student_enrolled_on NULLS LAST, id), 3) AS student_enrolled_on_cume_dist,
        LAG(score) OVER (PARTITION BY term ORDER BY score, id) AS prev_score,
        ROUND(score - AVG(score) OVER (PARTITION BY MONTH(enrolled_at), course_id), 2) AS score_vs_enrolled_at_month_course_id_avg,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at), course_id) AS rows_in_enrolled_at_month_course_id,
        ROUND(CUME_DIST() OVER (PARTITION BY student_cohort, course_id ORDER BY score, id), 3) AS score_cume_dist,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        MAX(score) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id, term), 0), 4) AS score_share_of_course_id_term
    FROM with_parent_student
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_enrolled_at_month_course_id AS (PARTITION BY MONTH(enrolled_at), course_id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    student_enrolled_on,
    seq_no,
    score_share_of_enrolled_at_month_course_id,
    student_enrolled_on_cume_dist,
    prev_score,
    score_vs_enrolled_at_month_course_id_avg,
    rows_in_enrolled_at_month_course_id,
    score_cume_dist,
    score_max_2,
    score_share_of_course_id_term,
    prev_score - score_cume_dist AS prev_score_minus_score_cume_dist,
    CASE WHEN student_cohort IN ('C2023', 'C2022') THEN 'group_a' WHEN student_cohort = 'C2021' THEN 'group_b' ELSE 'other' END AS student_cohort_group
FROM metrics
