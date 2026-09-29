WITH valid_enrollments AS (
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
    WHERE score >= 5.9
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        c.level AS course_level,
        c.dept AS course_dept,
        c.credits AS course_credits,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on
    FROM valid_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
grouped AS (
    -- Group-level totals
    SELECT
        student_id,
        MIN(score) AS min_score,
        ROUND(100 * COUNT(CASE WHEN course_dept IN ('Math', 'CS') THEN 1 END) / COUNT(*), 1) AS pct_course_dept_in_math_cs,
        ROUND(SUM(score) / NULLIF(SUM(student_gpa), 0), 3) AS score_to_student_gpa_ratio,
        ROUND(STDDEV(student_gpa), 2) AS stddev_student_gpa,
        ROUND(100 * COUNT(CASE WHEN course_dept NOT IN ('Art', 'Bio') THEN 1 END) / COUNT(*), 1) AS pct_course_dept_not_in_art_bio,
        ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list
    FROM combined_details
    GROUP BY 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(pct_course_dept_in_math_cs - AVG(pct_course_dept_in_math_cs) OVER (PARTITION BY student_id), 2) AS pct_course_dept_in_math_cs_vs_student_id_avg,
        ROUND(score_to_student_gpa_ratio / NULLIF(SUM(score_to_student_gpa_ratio) OVER (PARTITION BY student_id), 0), 4) AS score_to_student_gpa_ratio_share_of_student_id,
        MAX(pct_course_dept_in_math_cs) OVER w_student_id AS student_id_max_pct_course_dept_in_math_cs,
        ROUND(score_to_student_gpa_ratio - AVG(score_to_student_gpa_ratio) OVER w_student_id, 2) AS score_to_student_gpa_ratio_vs_student_id_avg
    FROM grouped
    WINDOW w_student_id AS (PARTITION BY student_id)
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.min_score,
        s.score_to_student_gpa_ratio,
        s.pct_course_dept_in_math_cs_vs_student_id_avg,
        s.student_id_max_pct_course_dept_in_math_cs,
        s.score_to_student_gpa_ratio_vs_student_id_avg,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_trends s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
with_parent_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.student_id,
        s.min_score,
        s.score_to_student_gpa_ratio,
        s.pct_course_dept_in_math_cs_vs_student_id_avg,
        s.student_id_max_pct_course_dept_in_math_cs,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_term,
        s.enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at_2,
        st.cohort AS student_cohort
    FROM with_children s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    student_id,
    score_to_student_gpa_ratio,
    pct_course_dept_in_math_cs_vs_student_id_avg,
    enrollment_grade_status,
    enrollment_enrolled_at,
    enrollment_score,
    enrollment_course_id,
    enrollment_enrolled_at_2,
    student_cohort,
    HOUR(enrollment_enrolled_at) AS enrollment_enrolled_at_hour
FROM with_parent_enrollment
ORDER BY enrollment_score NULLS LAST, student_id DESC
