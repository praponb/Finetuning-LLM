WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 73.7 THEN 'HIGH' WHEN score >= 17.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        CASE WHEN grade_status IN ('FAIL', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE course_id >= 11
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.score_tier,
        s.grade_status_group,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa,
        st.major AS student_major,
        c.credits AS course_credits
    FROM cleaned_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY score_tier, student_id ORDER BY course_credits ASC NULLS LAST, id), 3) AS course_credits_percent_rank,
        COUNT(*) OVER (PARTITION BY course_credits) AS rows_in_course_credits,
        SIZE(COLLECT_SET(grade_status_group) OVER w_course_id_student_is_interna) AS distinct_grade_status_group_in_course_id_student,
        FIRST_VALUE(course_credits) OVER (PARTITION BY student_major ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        ROW_NUMBER() OVER (PARTITION BY student_is_international, course_credits ORDER BY score DESC, id) AS score_seq,
        RANK() OVER (PARTITION BY student_is_international, course_credits ORDER BY score DESC) AS score_rank,
        NTILE(3) OVER (PARTITION BY student_is_international, course_credits ORDER BY score DESC, id) AS score_tercile,
        LAG(student_gpa) OVER (PARTITION BY student_is_international, course_credits ORDER BY score DESC, id) AS prev_student_gpa,
        SIZE(COLLECT_SET(score_tier) OVER (PARTITION BY course_id, student_is_international)) AS distinct_score_tier_in_course_id_student_is_inte,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, student_is_international), 2) AS score_vs_course_id_student_is_international_avg,
        SIZE(COLLECT_SET(term) OVER w_course_credits) AS distinct_term_in_course_credits,
        SUM(student_gpa) OVER (PARTITION BY course_credits) AS course_credits_sum_student_gpa,
        DENSE_RANK() OVER (PARTITION BY score_tier, student_id ORDER BY course_credits ASC NULLS LAST) AS course_credits_dense_rank,
        SUM(student_gpa) OVER (PARTITION BY student_is_international, course_credits ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS student_gpa_sum_3_prior,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER w_course_credits, 0), 4) AS course_credits_share_of_course_credits
    FROM with_parent
    WINDOW w_course_id_student_is_interna AS (PARTITION BY course_id, student_is_international), w_course_credits AS (PARTITION BY course_credits)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(student_major) OVER w_distinct_term_in_course_cred) AS distinct_student_major_in_distinct_term_in_cours,
        COUNT(*) OVER (PARTITION BY distinct_term_in_course_credits, grade_status_group) AS rows_in_distinct_term_in_course_credits_grade_st
    FROM ranked
    WINDOW w_distinct_term_in_course_cred AS (PARTITION BY distinct_term_in_course_credits, grade_status_group)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    score_tier,
    grade_status_group,
    student_is_international,
    student_gpa,
    student_major,
    course_credits_percent_rank,
    rows_in_course_credits,
    score_seq,
    score_rank,
    score_tercile,
    distinct_score_tier_in_course_id_student_is_inte,
    score_vs_course_id_student_is_international_avg,
    distinct_term_in_course_credits,
    course_credits_sum_student_gpa,
    student_gpa_sum_3_prior,
    distinct_student_major_in_distinct_term_in_cours,
    rows_in_distinct_term_in_course_credits_grade_st,
    CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
FROM windowed
ORDER BY score_rank DESC NULLS FIRST
