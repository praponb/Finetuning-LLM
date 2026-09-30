WITH filtered AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LENGTH(TRIM(cohort)) AS cohort_len
    FROM students
    WHERE gpa >= 2.7
),
unioned AS (
    -- Stack the branches
    SELECT cohort_len AS key_id, enrolled_on AS event_date
    FROM filtered
    UNION ALL
    SELECT id AS key_id, enrolled_on AS event_date
    FROM students
    WHERE cohort NOT IN ('C2023', 'C2024')
    UNION ALL
    SELECT id AS key_id, enrolled_on AS event_date
    FROM students
)
SELECT key_id, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY key_id
ORDER BY key_id
