WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 56.1) AS score_distance,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_distance,
        s.term_slug,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa,
        c.dept AS course_dept
    FROM valid s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE student_gpa > (SELECT MIN(student_gpa) FROM enriched)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        score_distance,
        term_slug,
        student_enrolled_on,
        student_cohort,
        student_gpa,
        course_dept,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY course_id, course_dept), 0), 4) AS score_distance_share_of_course_id_course_dept,
        MAX(student_gpa) OVER w_course_id_course_dept AS course_id_course_dept_max_student_gpa
    FROM with_activity
    WINDOW w_course_id_course_dept AS (PARTITION BY course_id, course_dept)
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    COALESCE(course_dept, 'ALL') AS course_dept,
    MIN(student_enrolled_on) AS first_student_enrolled_on
FROM with_windows
GROUP BY GROUPING SETS ((course_id, course_dept), (course_id), (course_dept))
