WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 76.8 THEN 'large' WHEN score >= 38.1 THEN 'medium' ELSE 'small' END AS score_tier,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE course_id <= 6
      AND score BETWEEN 32.9 AND 85.2
),
stats AS (
    -- Summarise per group
    SELECT
        grade_status,
        course_id,
        COUNT(*) FILTER (WHERE term IN ('2025T1', '2024T2', '2025T2')) AS n_term_in_2025t1_2024t2_2025t2,
        SUM(CASE WHEN grade_status <> 'INCOMPLETE' THEN 1 ELSE 0 END) AS n_grade_status_incomplete,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null
    FROM scoped_enrollments
    GROUP BY 1, 2
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status IS NOT NULL THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM stats s
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.grade_status,
        s.course_id,
        s.n_grade_status_is_not_null,
        s.enrollment_grade_status_count,
        s.last_enrollment_enrolled_at,
        c.level AS course_level
    FROM with_counts s
    JOIN courses c ON c.id = s.course_id
)
SELECT grade_status, course_id, field, amount
FROM (
    SELECT grade_status, course_id, enrollment_grade_status_count, n_grade_status_is_not_null
    FROM joined
) src
UNPIVOT (
    amount FOR field IN (enrollment_grade_status_count AS m_enrollment_grade_status_count, n_grade_status_is_not_null AS m_n_grade_status_is_not_null)
) unp
