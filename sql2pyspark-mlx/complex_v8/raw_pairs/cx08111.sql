WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 73.7 THEN 'HIGH' WHEN score >= 42.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE course_id <= 11
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.student_name,
        st.enrolled_on AS student_enrolled_on,
        c.course_code
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY YEAR(student_enrolled_on) ORDER BY student_gpa DESC NULLS FIRST, id) AS student_gpa_quintile,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_month_student_id) AS distinct_course_id_in_enrolled_at_month_student_,
        COUNT(*) OVER w_enrolled_at_month_student_id AS rows_in_enrolled_at_month_student_id,
        RANK() OVER (PARTITION BY YEAR(student_enrolled_on) ORDER BY student_gpa DESC NULLS FIRST) AS student_gpa_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY YEAR(student_enrolled_on) ORDER BY student_gpa DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM joined_student
    WINDOW w_enrolled_at_month_student_id AS (PARTITION BY MONTH(enrolled_at), student_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        student_enrolled_on,
        student_gpa_quintile,
        distinct_course_id_in_enrolled_at_month_student_,
        student_gpa_rank,
        ROUND(student_gpa_rank - AVG(student_gpa_rank) OVER w_student_gpa_rank_student_enr, 2) AS student_gpa_rank_vs_student_gpa_rank_student_enr,
        ROW_NUMBER() OVER (ORDER BY distinct_course_id_in_enrolled_at_month_student_ ASC, id) AS distinct_course_id_in_enrolled_at_month_student__2,
        LAG(student_enrolled_on) OVER (ORDER BY distinct_course_id_in_enrolled_at_month_student_ ASC, id) AS prev_student_enrolled_on
    FROM ranked
    WINDOW w_student_gpa_rank_student_enr AS (PARTITION BY student_gpa_rank, MONTH(student_enrolled_on))
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.student_enrolled_on,
        s.student_gpa_quintile,
        s.distinct_course_id_in_enrolled_at_month_student_,
        s.student_gpa_rank,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM sequenced s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
-- Subtotals with ROLLUP
SELECT
    enrollment_term,
    distinct_course_id_in_enrolled_at_month_student_,
    PERCENTILE_APPROX(enrollment_score, 0.5) AS median_enrollment_score,
    SUM(CASE WHEN enrollment_term = '2024T1' THEN 1 ELSE 0 END) AS n_enrollment_term_2024t1,
    SUM(distinct_course_id_in_enrolled_at_month_student_) AS total_distinct_course_id_in_enrolled_at_month_st
FROM with_children_enrollments
GROUP BY ROLLUP(enrollment_term, distinct_course_id_in_enrolled_at_month_student_)
ORDER BY enrollment_term NULLS LAST, distinct_course_id_in_enrolled_at_month_student_ NULLS LAST
