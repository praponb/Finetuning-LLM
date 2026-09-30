WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE course_id >= 7
      AND score <= 57.8
),
latest_per_key AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_dayofweek,
        grade_status_clean,
        enrolled_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at_dayofweek DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
)
SELECT
    course_id,
    term,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM latest_per_key
GROUP BY CUBE(course_id, term)
ORDER BY course_id NULLS LAST, term NULLS LAST
