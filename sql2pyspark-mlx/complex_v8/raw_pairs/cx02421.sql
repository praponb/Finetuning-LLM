WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        score_filled,
        grade_status_term_label,
        CONCAT(grade_status_term_label, '#', CAST(enrolled_at_year AS STRING)) AS grade_status_term_label_ref,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label_2,
        ROUND(score * 1.1, 2) AS score_adj
    FROM recent_enrollments
),
grouped AS (
    -- Group-level totals
    SELECT
        course_id,
        student_id,
        grade_status,
        COUNT(*) AS row_count,
        MIN(score) AS min_score,
        COUNT(*) FILTER (WHERE grade_status <> 'INCOMPLETE') AS n_grade_status_incomplete
    FROM annotated
    WHERE score > 20.2
    GROUP BY course_id, student_id, grade_status
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY n_grade_status_incomplete ASC NULLS LAST, course_id, student_id, grade_status) AS n_grade_status_incomplete_quintile,
        ROW_NUMBER() OVER (PARTITION BY course_id, grade_status ORDER BY n_grade_status_incomplete, course_id, student_id, grade_status) AS seq_no,
        n_grade_status_incomplete - LAG(n_grade_status_incomplete) OVER (PARTITION BY student_id ORDER BY n_grade_status_incomplete DESC, course_id, student_id, grade_status) AS n_grade_status_incomplete_change,
        min_score - LAG(min_score) OVER (PARTITION BY course_id, grade_status ORDER BY n_grade_status_incomplete, course_id, student_id, grade_status) AS min_score_change
    FROM grouped
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.course_id,
        s.student_id,
        s.grade_status,
        s.row_count,
        s.n_grade_status_incomplete,
        s.seq_no,
        s.n_grade_status_incomplete_change,
        s.min_score_change,
        c.credits AS course_credits,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort
    FROM ranked s
    JOIN courses c ON c.id = s.course_id AND c.level NOT IN ('intro', 'advanced')
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        course_id,
        student_id,
        grade_status,
        n_grade_status_incomplete,
        course_credits,
        student_is_international,
        student_cohort,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id, grade_status)) AS distinct_student_id_in_course_id_grade_status,
        LAG(row_count, 1, 0) OVER (PARTITION BY student_cohort, row_count ORDER BY row_count ASC, course_id, student_id, grade_status) AS prev_row_count
    FROM with_parent_course
),
with_details_student AS (
    -- Join students
    SELECT
        s.course_id,
        s.student_id,
        s.grade_status,
        s.n_grade_status_incomplete,
        s.course_credits,
        s.student_cohort,
        s.prev_row_count,
        st.student_name,
        st.gpa AS student_gpa,
        st.major AS student_major,
        c.level AS course_level
    FROM sequenced s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    course_id,
    student_id,
    grade_status,
    n_grade_status_incomplete,
    course_credits,
    student_cohort,
    prev_row_count,
    student_name,
    student_gpa,
    course_level
FROM with_details_student
ORDER BY student_id, student_name NULLS LAST, course_id, grade_status
LIMIT 20
