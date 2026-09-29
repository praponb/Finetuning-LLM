WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 63.9 THEN 'HIGH' WHEN score >= 32.7 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 13
      AND score < 77.0
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM valid_enrollments
    WHERE student_id >= 11
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.score_tier,
        s.grade_status_term_label,
        s.grade_status_group,
        s.days_since_enrolled_at,
        c.dept AS course_dept,
        c.course_code,
        c.credits AS course_credits
    FROM scored s
    LEFT JOIN courses c ON c.id = s.course_id AND c.credits >= 2
)
SELECT id, metric_name, value
FROM (
    SELECT id, days_since_enrolled_at, course_credits
    FROM joined_course
) src
UNPIVOT (
    value FOR metric_name IN (days_since_enrolled_at, course_credits)
) unp
ORDER BY id, metric_name
