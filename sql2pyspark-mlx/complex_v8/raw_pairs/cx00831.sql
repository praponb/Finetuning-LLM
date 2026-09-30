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
        CASE WHEN score >= 74.9 THEN 'A' WHEN score >= 32.9 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id >= 3
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2025T1'
        )
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_tier,
        c.dept AS course_dept,
        c.course_code
    FROM qualified s
    JOIN courses c ON c.id = s.course_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        term,
        COUNT(*) AS row_count,
        SUM(CASE WHEN course_dept NOT IN ('Art', 'Biz') THEN 1 ELSE 0 END) AS n_course_dept_not_in_art_biz,
        COUNT(DISTINCT score_tier) AS distinct_score_tiers
    FROM with_details
    GROUP BY student_id, term
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(term, '#', CAST(distinct_score_tiers AS STRING)) AS term_ref,
        distinct_score_tiers - row_count AS distinct_score_tiers_minus_row_count
    FROM totals
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.term,
        s.n_course_dept_not_in_art_biz,
        s.distinct_score_tiers,
        s.term_ref,
        s.distinct_score_tiers_minus_row_count,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM annotated s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
combined_details_enrollment AS (
    -- Enrich with dimension data
    SELECT
        s.student_id,
        s.term,
        s.n_course_dept_not_in_art_biz,
        s.distinct_score_tiers,
        s.term_ref,
        s.distinct_score_tiers_minus_row_count,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_score,
        s.enrollment_grade_status,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status_2,
        c.level AS course_level,
        c.credits AS course_credits
    FROM exploded_detail_enrollments s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = s.enrollment_course_id
    WHERE e.grade_status = 'FAIL'
)
SELECT
    student_id,
    term,
    n_course_dept_not_in_art_biz,
    term_ref,
    distinct_score_tiers_minus_row_count,
    enrollment_id,
    enrollment_course_id,
    enrollment_grade_status,
    enrollment_term,
    course_level,
    course_credits
FROM combined_details_enrollment
ORDER BY enrollment_course_id
