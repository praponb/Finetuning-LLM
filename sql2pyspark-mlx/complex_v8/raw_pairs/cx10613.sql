WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(term)) AS term_len,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM scoped_enrollments
    WHERE term IN ('2024T1', '2025T1')
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_2,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_3
    FROM labeled s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, term_len, enrollment_count_2
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (term_len, enrollment_count_2)
) unp
