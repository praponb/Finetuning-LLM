WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 62.5 THEN 'large' WHEN score >= 36.6 THEN 'medium' ELSE 'small' END AS score_tier,
        LENGTH(TRIM(term)) AS term_len,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE (course_id IS NOT NULL OR grade_status IS NOT NULL)
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        s.term_or_unknown,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM cleaned_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        s.term_or_unknown,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_student_id,
        st.gpa AS student_gpa,
        st.major AS student_major,
        st.is_international AS student_is_international,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM line_level s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(score_tier, 'ALL') AS score_tier,
    COALESCE(student_major, 'ALL') AS student_major,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(score), 2) AS stddev_score
FROM enriched
GROUP BY GROUPING SETS ((score_tier, student_major), (score_tier))
