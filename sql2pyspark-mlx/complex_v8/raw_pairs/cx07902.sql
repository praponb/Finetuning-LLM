WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score > 38.3
      AND course_id <= 10
),
with_parent_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major
    FROM scoped s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on BETWEEN DATE '2024-03-15' AND DATE '2025-06-27'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        student_enrolled_on,
        COUNT(*) OVER (PARTITION BY student_major, term) AS rows_in_student_major_term,
        ROUND(CUME_DIST() OVER w_grade_status_by_score_ties, 3) AS score_cume_dist,
        LEAD(student_enrolled_on) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at ASC, id) AS next_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY score DESC NULLS FIRST) AS score_dense_rank,
        LAG(enrolled_at) OVER (PARTITION BY term ORDER BY score DESC NULLS FIRST, id) AS prev_enrolled_at,
        SIZE(COLLECT_SET(course_id) OVER w_term_student_id) AS distinct_course_id_in_term_student_id,
        ROUND(score - AVG(score) OVER (PARTITION BY student_major, term), 2) AS score_vs_student_major_term_avg,
        LAG(student_enrolled_on, 3) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at ASC, id) AS prev3_student_enrolled_on,
        SIZE(COLLECT_SET(student_major) OVER w_term_student_id) AS distinct_student_major_in_term_student_id
    FROM with_parent_student
    WINDOW w_grade_status_by_score_ties AS (PARTITION BY grade_status ORDER BY score), w_term_student_id AS (PARTITION BY term, student_id), w_student_major_term AS (PARTITION BY student_major, term)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(score_cume_dist) OVER (PARTITION BY term ORDER BY prev_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_cume_dist_so_far,
        NTILE(10) OVER (PARTITION BY distinct_student_major_in_term_student_id, term ORDER BY student_enrolled_on NULLS LAST, id) AS student_enrolled_on_decile,
        ROW_NUMBER() OVER (PARTITION BY distinct_student_major_in_term_student_id ORDER BY distinct_student_major_in_term_student_id, id) AS rn
    FROM with_trends
)
SELECT
    id,
    course_id,
    student_enrolled_on,
    rows_in_student_major_term,
    score_cume_dist,
    score_dense_rank,
    prev_enrolled_at,
    distinct_course_id_in_term_student_id,
    score_vs_student_major_term_avg,
    prev3_student_enrolled_on,
    distinct_student_major_in_term_student_id,
    min_score_cume_dist_so_far,
    student_enrolled_on_decile,
    rn,
    CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
    ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_prev_enrolled_at
FROM with_trends_2
WHERE score_cume_dist > 0.3
ORDER BY student_enrolled_on DESC, rows_in_student_major_term DESC
