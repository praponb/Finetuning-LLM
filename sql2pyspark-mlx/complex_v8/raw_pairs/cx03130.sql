WITH filtered AS (
    -- Scope the students rows
    SELECT *
    FROM students
    WHERE cohort = 'C2023'
      AND major IN ('Biz', 'Bio')
),
deduped AS (
    -- Keep the latest row per major, cohort
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major, cohort ORDER BY enrolled_on DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
major_stats AS (
    -- Max gpa per major
    SELECT major, MAX(gpa) AS max_gpa
    FROM deduped
    GROUP BY major
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_gpa, ROUND(s.gpa - g.max_gpa, 2) AS gpa_minus_max
    FROM deduped s
    JOIN major_stats g ON g.major = s.major
    WHERE s.gpa = g.max_gpa
)
-- Final projection
SELECT
    id,
    student_name,
    major,
    enrolled_on,
    gpa,
    is_international,
    max_gpa,
    gpa_minus_max,
    LENGTH(TRIM(major)) AS major_len,
    COALESCE(gpa, 0.0) AS gpa_filled
FROM above_average
WHERE gpa BETWEEN 2.1 AND 3.4
ORDER BY id DESC NULLS FIRST, gpa_minus_max DESC NULLS FIRST
