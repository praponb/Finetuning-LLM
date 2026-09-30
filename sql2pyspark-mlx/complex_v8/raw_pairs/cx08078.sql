WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE (course_id IS NOT NULL OR student_id IS NOT NULL)
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_slug,
        s.term_ref,
        c.course_code,
        st.gpa AS student_gpa
    FROM prepared s
    LEFT JOIN courses c ON c.id = s.course_id AND c.credits > 2
    JOIN students st ON st.id = s.student_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM joined_course s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id <= 5
              AND MONTH(e.enrolled_at) IN (2, 3)
        )
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term = '2025T2'
        )
      AND MONTH(s.enrolled_at) IN (1, 2)
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_slug,
        s.course_code,
        s.student_gpa,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM qualified s
    JOIN enrollments e ON e.student_id = s.student_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        enrollment_grade_status,
        CASE WHEN score >= 56.1 THEN 'large' WHEN score >= 36.6 THEN 'medium' ELSE 'small' END AS score_tier,
        enrollment_course_id,
        COUNT(*) AS cnt,
        SUM(CASE WHEN LENGTH(TRIM(course_code)) >= 4 THEN score ELSE 0 END) AS score_length_trim_course_code_4,
        COUNT(DISTINCT enrollment_course_id) AS distinct_enrollment_course_ids,
        ARRAY_SORT(COLLECT_SET(enrollment_grade_status)) AS enrollment_grade_status_list,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM exploded_detail_enrollments
    GROUP BY 1, 2, 3
)
SELECT
    enrollment_grade_status,
    score_tier,
    enrollment_course_id,
    cnt,
    score_length_trim_course_code_4,
    distinct_enrollment_course_ids,
    enrollment_grade_status_list,
    distinct_grade_status
FROM rollup_base
ORDER BY distinct_enrollment_course_ids DESC
