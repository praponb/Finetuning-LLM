WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2025T1') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        CASE WHEN score >= 80.2 THEN 'HIGH' WHEN score >= 36.6 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE score > 32.7
      AND HOUR(enrolled_at) BETWEEN 7 AND 13
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM valid_enrollments
    WHERE student_id <= 6
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN grade_status_or_unknown IN ('INCOMPLETE', 'WITHDRAWN') THEN 'group_a' WHEN grade_status_or_unknown = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_or_unknown_group,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM tagged
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IN ('FAIL', 'PASS', 'WITHDRAWN')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM enriched s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, enrollment_grade_status_count, enrollment_count
    FROM with_stats
) src
UNPIVOT (
    value FOR field IN (enrollment_grade_status_count AS m_enrollment_grade_status_count, enrollment_count AS m_enrollment_count)
) unp
