WITH scoped AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(major), ' ', '_')) AS major_slug
    FROM students
    WHERE YEAR(enrolled_on) = 2024
      AND gpa >= 2.7
)
SELECT
    id,
    gpa,
    enrolled_on,
    'student' AS origin
FROM scoped
WHERE major IN ('Biz', 'CS')
UNION
SELECT
    id,
    gpa,
    enrolled_on,
    'student_2' AS origin
FROM students
WHERE enrolled_on IS NOT NULL
ORDER BY id DESC
