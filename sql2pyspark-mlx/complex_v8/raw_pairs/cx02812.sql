WITH valid_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        ROUND(gpa * 0.9, 2) AS gpa_adj
    FROM students
    WHERE enrolled_on IS NOT NULL
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid_students
    WHERE gpa > (SELECT AVG(gpa) FROM valid_students)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_enrolled_on_ties AS enrolled_on_dense_rank,
        COUNT(*) OVER (ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(3) OVER (ORDER BY enrolled_on DESC, id) AS enrolled_on_tercile,
        LEAD(gpa_adj) OVER w_major_by_gpa AS next_gpa_adj,
        LAST_VALUE(gpa_adj) IGNORE NULLS OVER (ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_gpa_adj,
        ROUND(AVG(gpa_adj) OVER (ORDER BY enrolled_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS gpa_adj_avg_7
    FROM screened
    WINDOW w_all_by_enrolled_on_ties AS (ORDER BY enrolled_on DESC), w_major_by_gpa AS (PARTITION BY major ORDER BY gpa DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_on_tercile, major, days_since_enrolled_on
    FROM windowed
) src
PIVOT (
    SUM(days_since_enrolled_on) AS total, COUNT(days_since_enrolled_on) AS n
    FOR major IN ('Art', 'CS', 'Biz', 'Bio')
)
