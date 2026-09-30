WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE student_id <= 13
),
deduped AS (
    -- Keep the latest row per course_id, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id, grade_status ORDER BY score DESC, id DESC) AS row_num
        FROM prepared_enrollments
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'FAIL' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_fail,
        ROUND(100 * COUNT(CASE WHEN grade_status IN ('FAIL', 'PASS') THEN 1 END) / COUNT(*), 1) AS pct_grade_status_in_fail_pass,
        COUNT(DISTINCT id) AS distinct_ids
    FROM deduped
    WHERE grade_status = 'FAIL'
    GROUP BY 1
    HAVING COUNT(*) >= 3
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status = 'PASS' THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at
    FROM totals s
),
with_stats_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_2
    FROM with_stats s
)
SELECT
    distinct_ids,
    enrollment_grade_status_count,
    SUM(enrollment_count_2) AS total_enrollment_count_2
FROM with_stats_2
GROUP BY GROUPING SETS ((distinct_ids, enrollment_grade_status_count), (distinct_ids), (enrollment_grade_status_count), ())
