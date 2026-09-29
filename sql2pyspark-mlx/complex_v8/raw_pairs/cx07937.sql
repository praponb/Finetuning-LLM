WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE (enrolled_at >= TIMESTAMP '2025-01-13 11:00:00' OR course_id IS NOT NULL)
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_group,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa,
        c.dept AS course_dept
    FROM recent s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(score) OVER (PARTITION BY course_dept) AS course_dept_min_score,
        ROUND(AVG(student_gpa) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS student_gpa_avg_7,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id, grade_status_group)) AS distinct_grade_status_in_course_id_grade_status_,
        DENSE_RANK() OVER w_course_dept_by_enrolled_at AS enrolled_at_dense_rank,
        SUM(student_gpa) OVER w_course_id_grade_status_group AS course_id_grade_status_group_sum_student_gpa,
        ROW_NUMBER() OVER w_grade_status_group_by_score AS score_seq,
        MAX(student_gpa) OVER (PARTITION BY course_id, grade_status_group) AS course_id_grade_status_group_max_student_gpa,
        COUNT(*) OVER (PARTITION BY term, student_cohort ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(student_gpa) OVER w_course_dept AS course_dept_max_student_gpa,
        ROUND(AVG(score) OVER (PARTITION BY course_dept), 2) AS course_dept_avg_score,
        LAST_VALUE(student_gpa) OVER (PARTITION BY course_dept ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_gpa,
        SUM(student_gpa) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS student_gpa_sum_2,
        SIZE(COLLECT_SET(grade_status_group) OVER w_course_dept) AS distinct_grade_status_group_in_course_dept,
        ROUND(CUME_DIST() OVER (PARTITION BY course_dept ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        RANK() OVER (PARTITION BY term, student_cohort ORDER BY student_gpa NULLS LAST) AS student_gpa_rank
    FROM with_parent_student
    WINDOW w_course_dept_by_enrolled_at AS (PARTITION BY course_dept ORDER BY enrolled_at, id), w_course_id_grade_status_group AS (PARTITION BY course_id, grade_status_group), w_grade_status_group_by_score AS (PARTITION BY grade_status_group ORDER BY score, id), w_course_dept AS (PARTITION BY course_dept)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_group,
        student_cohort,
        student_gpa,
        course_dept,
        course_dept_min_score,
        student_gpa_avg_7,
        distinct_grade_status_in_course_id_grade_status_,
        enrolled_at_dense_rank,
        course_id_grade_status_group_sum_student_gpa,
        course_id_grade_status_group_max_student_gpa,
        running_count,
        course_dept_max_student_gpa,
        course_dept_avg_score,
        last_student_gpa,
        student_gpa_sum_2,
        distinct_grade_status_group_in_course_dept,
        enrolled_at_cume_dist,
        student_gpa_rank
    FROM with_windows
    WHERE score_seq = 1
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    student_gpa,
    course_dept,
    course_dept_min_score,
    student_gpa_avg_7,
    distinct_grade_status_in_course_id_grade_status_,
    enrolled_at_dense_rank,
    course_id_grade_status_group_sum_student_gpa,
    course_id_grade_status_group_max_student_gpa,
    running_count,
    course_dept_max_student_gpa,
    student_gpa_sum_2,
    distinct_grade_status_group_in_course_dept,
    enrolled_at_cume_dist,
    student_gpa_rank,
    enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d,
    LENGTH(TRIM(course_dept)) AS course_dept_len
FROM first_rows
ORDER BY course_dept_len, id
