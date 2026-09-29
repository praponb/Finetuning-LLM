WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        ROUND(score * 0.25, 2) AS score_adj,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE course_id >= 6
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at_2
    FROM cleaned_enrollments s
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(first_enrollment_enrolled_at) AS first_enrollment_enrolled_at_day,
        term,
        grade_status,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ROUND(SUM(score) / NULLIF(SUM(grade_status_len), 0), 3) AS score_to_grade_status_len_ratio,
        SUM(CASE WHEN grade_status = 'FAIL' THEN 1 ELSE 0 END) AS n_grade_status_fail
    FROM profiled
    GROUP BY 1, 2, 3
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE n_grade_status_fail > (SELECT AVG(n_grade_status_fail) FROM grouped)
)
-- Final projection
SELECT
    first_enrollment_enrolled_at_day,
    term,
    grade_status,
    term_list
FROM without_matches
