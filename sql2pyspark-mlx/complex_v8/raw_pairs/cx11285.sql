WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 13.9 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE grade_status = 'PASS'
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        c.course_code,
        c.level AS course_level,
        c.credits AS course_credits,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
    WHERE c.credits >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY course_credits ORDER BY score, id) AS rn,
        course_credits - LAG(course_credits) OVER w_student_cohort_student_id_by AS course_credits_change,
        COUNT(*) OVER (ORDER BY student_gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(student_gpa) OVER w_student_id, 2) AS student_id_avg_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER w_grade_status_student_id, 2) AS student_gpa_vs_grade_status_student_id_avg,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS score_vs_grade_status_student_id_avg,
        RANK() OVER (ORDER BY student_gpa DESC) AS student_gpa_rank,
        course_credits - LAG(course_credits) OVER (PARTITION BY term, grade_status ORDER BY course_credits, id) AS course_credits_change_2,
        LEAD(score) OVER (PARTITION BY course_credits ORDER BY score, id) AS next_score,
        ROUND(student_gpa - AVG(student_gpa) OVER w_student_id, 2) AS student_gpa_vs_student_id_avg
    FROM enriched_course
    WINDOW w_student_cohort_student_id_by AS (PARTITION BY student_cohort, student_id ORDER BY score DESC NULLS FIRST, id), w_student_id AS (PARTITION BY student_id), w_grade_status_student_id AS (PARTITION BY grade_status, student_id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(enrolled_at) AS enrolled_at_year,
        GREATEST(score, student_gpa) AS greatest_score_student_gpa,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM sequenced
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(student_id_avg_student_gpa) OVER (PARTITION BY student_gpa_rank, term ORDER BY score_vs_grade_status_student_id_avg, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS student_id_avg_student_gpa_max_2_prior,
        SUM(student_id_avg_student_gpa) OVER (PARTITION BY student_gpa_rank, term ORDER BY score_vs_grade_status_student_id_avg, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS student_id_avg_student_gpa_sum_6_prior,
        ROW_NUMBER() OVER (PARTITION BY grade_status, enrolled_at_year ORDER BY student_gpa, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY student_gpa_rank, term ORDER BY score_vs_grade_status_student_id_avg) AS score_vs_grade_status_student_id_avg_dense_rank,
        ROUND(AVG(student_gpa_rank) OVER (PARTITION BY student_gpa_rank, term ORDER BY score_vs_grade_status_student_id_avg, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS student_gpa_rank_avg_6_prior,
        NTH_VALUE(student_gpa, 2) OVER (PARTITION BY grade_status, enrolled_at_year ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa
    FROM tagged
)
SELECT
    id,
    student_id,
    term,
    score_tier,
    course_code,
    course_level,
    student_cohort,
    rn,
    course_credits_change,
    student_id_avg_student_gpa,
    student_gpa_vs_grade_status_student_id_avg,
    score_vs_grade_status_student_id_avg,
    student_gpa_rank,
    course_credits_change_2,
    next_score,
    student_gpa_vs_student_id_avg,
    greatest_score_student_gpa,
    enrolled_at_week,
    student_id_avg_student_gpa_max_2_prior,
    student_id_avg_student_gpa_sum_6_prior,
    seq_no,
    score_vs_grade_status_student_id_avg_dense_rank,
    student_gpa_rank_avg_6_prior,
    second_student_gpa,
    ROUND(student_gpa / NULLIF(enrolled_at_year, 0), 3) AS student_gpa_per_enrolled_at_year
FROM with_trends
