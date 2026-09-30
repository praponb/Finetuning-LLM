WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE term <> '2024T1'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        grade_status_term_label,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 32.9 THEN 'medium' ELSE 'small' END AS score_tier
    FROM filtered_enrollments
    WHERE term IS NOT NULL
),
student_id_stats AS (
    -- Median score per student_id
    SELECT student_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM derived
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM derived s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score >= g.median_score
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
        s.grade_status_term_label,
        s.median_score,
        s.score_minus_median,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort
    FROM compared s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort IS NOT NULL
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    enrolled_at,
    grade_status_term_label,
    median_score,
    score_minus_median,
    student_gpa,
    student_cohort,
    LOWER(TRIM(term)) AS term_clean,
    YEAR(enrolled_at) AS enrolled_at_year
FROM enriched
WHERE course_id <= 4
ORDER BY grade_status_term_label DESC NULLS FIRST
