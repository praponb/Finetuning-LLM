WITH filtered_enrollments AS (
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
    WHERE term NOT IN ('2025T2', '2024T1')
      AND student_id >= 2
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id AND st.major = 'Math'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        score - LAG(score) OVER w_student_id_student_cohort_by AS score_change,
        RANK() OVER (PARTITION BY student_cohort ORDER BY student_gpa ASC) AS student_gpa_rank,
        SUM(student_gpa) OVER (PARTITION BY student_id, student_cohort ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_student_gpa,
        ROUND(AVG(score) OVER (PARTITION BY MONTH(enrolled_at), student_is_international), 2) AS enrolled_at_month_student_is_international_avg_s,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(enrolled_at), student_is_international)) AS distinct_course_id_in_enrolled_at_month_student_,
        NTILE(10) OVER w_student_id_student_cohort_by AS score_decile,
        student_gpa - LAG(student_gpa) OVER (PARTITION BY student_id, student_cohort ORDER BY score, id) AS student_gpa_change,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id, student_cohort ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LAG(score, 2) OVER w_student_cohort_by_student_gp_2 AS prev2_score,
        ROUND(CUME_DIST() OVER (PARTITION BY student_cohort ORDER BY student_gpa ASC), 3) AS student_gpa_cume_dist,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_is_international, course_id ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        DENSE_RANK() OVER w_student_id_student_cohort_by_4 AS score_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY MONTH(enrolled_at), student_is_international), 0), 4) AS score_share_of_enrolled_at_month_student_is_inte,
        ROUND(PERCENT_RANK() OVER w_student_id_student_cohort_by, 3) AS score_percent_rank,
        LEAD(enrolled_at, 2) OVER (PARTITION BY student_cohort ORDER BY student_gpa ASC, id) AS next2_enrolled_at
    FROM combined_details_student
    WINDOW w_student_id_student_cohort_by AS (PARTITION BY student_id, student_cohort ORDER BY score, id), w_student_cohort_by_student_gp_2 AS (PARTITION BY student_cohort ORDER BY student_gpa ASC, id), w_student_id_student_cohort_by_4 AS (PARTITION BY student_id, student_cohort ORDER BY score)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN student_cohort IN ('C2024', 'C2022') THEN 'group_a' WHEN student_cohort = 'C2023' THEN 'group_b' ELSE 'other' END AS student_cohort_group,
        CASE WHEN student_cohort IN ('C2023', 'C2022') THEN 'group_a' WHEN student_cohort = 'C2021' THEN 'group_b' ELSE 'other' END AS student_cohort_group_2,
        ROUND(prev2_score * 1.07, 2) AS prev2_score_adj
    FROM with_trends
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    student_gpa,
    student_cohort,
    student_is_international,
    student_gpa_rank,
    running_student_gpa,
    enrolled_at_month_student_is_international_avg_s,
    student_gpa_change,
    second_score,
    prev2_score,
    student_gpa_cume_dist,
    last_enrolled_at,
    score_dense_rank,
    score_share_of_enrolled_at_month_student_is_inte,
    score_percent_rank,
    student_cohort_group_2,
    prev2_score_adj
FROM labeled
ORDER BY enrolled_at_month_student_is_international_avg_s, score_dense_rank DESC
