WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE (course_id <= 11 OR grade_status NOT IN ('FAIL', 'INCOMPLETE'))
),
with_details AS (
    -- Join courses
    SELECT s.*, c.course_code, c.dept AS course_dept
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept NOT IN ('Bio', 'Biz')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        term_ref,
        course_code,
        course_dept,
        ROW_NUMBER() OVER w_grade_status_by_score AS seq_no,
        ROUND(score - AVG(score) OVER w_course_dept_student_id, 2) AS score_vs_course_dept_student_id_avg,
        COUNT(*) OVER (PARTITION BY course_id, course_dept ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        COUNT(*) OVER (PARTITION BY course_dept, student_id) AS rows_in_course_dept_student_id,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY course_id)) AS distinct_course_dept_in_course_id,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_dept, student_id), 0), 4) AS score_share_of_course_dept_student_id,
        MIN(score) OVER (PARTITION BY course_dept, student_id) AS course_dept_student_id_min_score,
        NTILE(4) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_quartile,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        MAX(score) OVER w_course_id AS course_id_max_score,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg,
        ROW_NUMBER() OVER (PARTITION BY course_id, course_dept ORDER BY enrolled_at DESC, id) AS rn
    FROM with_details
    WINDOW w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score, id), w_course_dept_student_id AS (PARTITION BY course_dept, student_id), w_course_id AS (PARTITION BY course_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY distinct_course_dept_in_course_id, rn) AS distinct_course_dept_in_course_id_rn_sum_seq_no,
        SIZE(COLLECT_SET(grade_status) OVER w_distinct_course_dept_in_cour) AS distinct_grade_status_in_distinct_course_dept_in,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY distinct_course_dept_in_course_id, rn)) AS distinct_student_id_in_distinct_course_dept_in_c,
        NTILE(3) OVER (ORDER BY score_vs_course_dept_student_id_avg, id) AS score_vs_course_dept_student_id_avg_tercile
    FROM ranked
    WINDOW w_distinct_course_dept_in_cour AS (PARTITION BY distinct_course_dept_in_course_id, rn)
)
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    term_ref,
    course_code,
    course_dept,
    seq_no,
    score_vs_course_dept_student_id_avg,
    running_count,
    score_avg_7,
    rows_in_course_dept_student_id,
    score_share_of_course_dept_student_id,
    course_dept_student_id_min_score,
    enrolled_at_quartile,
    course_id_max_score,
    rn,
    distinct_course_dept_in_course_id_rn_sum_seq_no,
    distinct_grade_status_in_distinct_course_dept_in,
    distinct_student_id_in_distinct_course_dept_in_c,
    score_vs_course_dept_student_id_avg_tercile
FROM sequenced
ORDER BY distinct_course_dept_in_course_id_rn_sum_seq_no, course_dept_student_id_min_score DESC
