WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 34.2) AS score_distance
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-18'
      AND course_id >= 7
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_distance,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on BETWEEN DATE '2024-01-24' AND DATE '2025-07-18'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        student_cohort,
        student_enrolled_on,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(AVG(score_distance) OVER w_student_cohort, 2) AS student_cohort_avg_score_distance,
        MAX(score) OVER w_student_cohort AS student_cohort_max_score,
        ROUND(CUME_DIST() OVER w_grade_status_student_major_b, 3) AS score_distance_cume_dist,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        ROUND(score - AVG(score) OVER w_student_cohort, 2) AS score_vs_student_cohort_avg,
        SIZE(COLLECT_SET(student_major) OVER w_student_cohort) AS distinct_student_major_in_student_cohort,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY student_cohort), 2) AS score_distance_vs_student_cohort_avg,
        SUM(score) OVER (PARTITION BY student_cohort) AS student_cohort_sum_score,
        COUNT(*) OVER (PARTITION BY student_cohort) AS rows_in_student_cohort,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status, student_major ORDER BY score_distance ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        DENSE_RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST) AS enrolled_at_dense_rank,
        FIRST_VALUE(student_enrolled_on) IGNORE NULLS OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_student_enrolled_on
    FROM joined
    WINDOW w_student_cohort AS (PARTITION BY student_cohort), w_grade_status_student_major_b AS (PARTITION BY grade_status, student_major ORDER BY score_distance ASC NULLS LAST)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        score_distance,
        student_enrolled_on,
        second_enrolled_at,
        student_cohort_avg_score_distance,
        student_cohort_max_score,
        second_enrolled_at_2,
        score_vs_student_cohort_avg,
        distinct_student_major_in_student_cohort,
        score_distance_vs_student_cohort_avg,
        student_cohort_sum_score,
        rows_in_student_cohort,
        first_known_score,
        enrolled_at_dense_rank,
        first_known_student_enrolled_on,
        ROW_NUMBER() OVER (ORDER BY student_cohort_max_score ASC, id) AS student_cohort_max_score_seq,
        ROUND(CUME_DIST() OVER w_student_id_first_known_stude, 3) AS score_distance_cume_dist_cume_dist,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        score - LAG(score) OVER w_student_id_first_known_stude AS score_change,
        DENSE_RANK() OVER w_all_by_student_cohort_max_sc AS student_cohort_max_score_dense_rank,
        MIN(first_known_score) OVER (ORDER BY student_cohort_max_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_first_known_score_so_far,
        LAST_VALUE(enrolled_at_dense_rank) IGNORE NULLS OVER (PARTITION BY student_cohort, course_id ORDER BY first_known_student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_dense_rank
    FROM with_windows
    WINDOW w_student_id_first_known_stude AS (PARTITION BY student_id, MONTH(first_known_student_enrolled_on) ORDER BY score_distance_cume_dist DESC, id), w_all_by_student_cohort_max_sc AS (ORDER BY student_cohort_max_score ASC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    enrolled_at,
    score_distance,
    student_enrolled_on,
    second_enrolled_at,
    student_cohort_avg_score_distance,
    student_cohort_max_score,
    second_enrolled_at_2,
    score_vs_student_cohort_avg,
    distinct_student_major_in_student_cohort,
    score_distance_vs_student_cohort_avg,
    student_cohort_sum_score,
    rows_in_student_cohort,
    enrolled_at_dense_rank,
    student_cohort_max_score_seq,
    score_distance_cume_dist_cume_dist,
    score_change,
    min_first_known_score_so_far,
    last_known_enrolled_at_dense_rank,
    CASE WHEN score_change >= 10.0 THEN 'large' WHEN score_change >= -40.0 THEN 'medium' ELSE 'small' END AS score_change_tier
FROM with_trends
ORDER BY grade_status DESC
