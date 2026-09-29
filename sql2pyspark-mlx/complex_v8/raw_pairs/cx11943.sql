WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE score >= 34.2
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM recent
    WHERE score_bucket < (SELECT MAX(score_bucket) FROM recent)
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE score_bucket < (SELECT MAX(score_bucket) FROM with_activity)
)
-- Subtotals with ROLLUP
SELECT
    student_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING(student_id) AS is_total_student_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(score_bucket) FILTER (WHERE grade_status = 'PASS') AS score_bucket_grade_status_pass
FROM with_activity_2
GROUP BY ROLLUP(student_id, grade_status)
ORDER BY student_id NULLS LAST, grade_status NULLS LAST
