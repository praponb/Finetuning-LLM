WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE student_id <= 11
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_prefix,
        s.score_filled,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM recent_enrollments s
    JOIN students st ON st.id = s.student_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        ROUND(SUM(score), 2) AS total_score,
        PERCENTILE_APPROX(score_filled, 0.5) AS median_score_filled,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_incomplete,
        MIN(score_filled) AS min_score_filled,
        MIN(score) AS min_score
    FROM with_parent_student
    GROUP BY student_id
    HAVING AVG(score) > 20.6
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.pct_grade_status_incomplete,
        s.min_score_filled,
        s.min_score,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM totals s
    JOIN enrollments e ON e.student_id = s.student_id
),
enrollment_grade_status_stats AS (
    -- Avg min_score per enrollment_grade_status
    SELECT enrollment_grade_status, AVG(min_score) AS avg_min_score
    FROM exploded_detail_enrollments
    GROUP BY enrollment_grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_min_score,
        ROUND(s.min_score - g.avg_min_score, 2) AS min_score_minus_avg
    FROM exploded_detail_enrollments s
    JOIN enrollment_grade_status_stats g ON g.enrollment_grade_status = s.enrollment_grade_status
    WHERE s.min_score > g.avg_min_score
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.min_score_filled,
        s.min_score,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.avg_min_score,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.grade_status AS enrollment_grade_status_2
    FROM outliers s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    student_id,
    min_score,
    enrollment_id,
    enrollment_enrolled_at,
    avg_min_score,
    enrollment_id_2,
    enrollment_term,
    enrollment_enrolled_at_2,
    enrollment_grade_status_2
FROM exploded_detail
ORDER BY enrollment_enrolled_at NULLS LAST
