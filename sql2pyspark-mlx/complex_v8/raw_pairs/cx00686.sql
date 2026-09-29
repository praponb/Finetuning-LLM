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
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        ABS(score - 30.0) AS score_distance,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE student_id <= 14
      AND course_id IS NOT NULL
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.term_or_unknown,
        s.score_distance,
        s.days_since_enrolled_at,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        course_id,
        course_dept,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT student_major) AS distinct_student_majors,
        ROUND(AVG(days_since_enrolled_at), 2) AS avg_days_since_enrolled_at,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ROUND(SUM(days_since_enrolled_at) / NULLIF(SUM(score_distance), 0), 3) AS days_since_enrolled_at_to_score_distance_ratio,
        MIN(student_enrolled_on) AS first_student_enrolled_on
    FROM combined_details
    WHERE student_id >= 17
    GROUP BY student_id, course_id, course_dept
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, course_dept, distinct_student_majors
    FROM summary
) src
PIVOT (
    COUNT(distinct_student_majors)
    FOR course_dept IN ('Biz', 'CS', 'Art')
)
