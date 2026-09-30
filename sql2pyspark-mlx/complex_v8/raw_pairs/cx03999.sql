WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(score, 0.0) AS score_filled
    FROM valid
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        grade_status,
        grade_status_group,
        enrolled_at_plus_30m,
        DENSE_RANK() OVER (ORDER BY score DESC) AS score_dense_rank,
        FIRST_VALUE(enrolled_at_plus_30m) OVER (PARTITION BY grade_status_group, MONTH(enrolled_at_plus_30m) ORDER BY score_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_plus_30m,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status_group)) AS distinct_student_id_in_grade_status_group
    FROM labeled
)
SELECT
    id,
    student_id,
    score,
    grade_status_group,
    enrolled_at_plus_30m,
    score_dense_rank,
    first_enrolled_at_plus_30m,
    distinct_student_id_in_grade_status_group
FROM metrics
ORDER BY grade_status_group DESC
