WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score <= 73.7
      AND grade_status IN ('PASS', 'INCOMPLETE', 'FAIL')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id AND st.is_international = FALSE
),
grouped AS (
    -- Group-level totals
    SELECT
        student_id,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT student_id) AS distinct_student_ids
    FROM with_parent
    GROUP BY student_id
)
SELECT student_id, metric_name, reading
FROM (
    SELECT student_id, n_rows, distinct_student_ids
    FROM grouped
) src
UNPIVOT (
    reading FOR metric_name IN (n_rows, distinct_student_ids)
) unp
