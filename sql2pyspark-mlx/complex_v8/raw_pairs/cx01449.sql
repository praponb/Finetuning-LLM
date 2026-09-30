WITH prepared_students AS (
    -- Scope the students rows
    SELECT
        id,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE student_name IS NOT NULL
      AND enrolled_on >= DATE '2024-10-09'
),
stats AS (
    -- Summarise per group
    SELECT
        is_international,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_ym,
        ROUND(AVG(gpa), 2) AS avg_gpa,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows
    FROM prepared_students
    GROUP BY is_international, DATE_FORMAT(enrolled_on, 'yyyy-MM')
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        is_international,
        enrolled_on_ym,
        distinct_ids,
        n_rows,
        SUM(avg_gpa) OVER (PARTITION BY n_rows) AS n_rows_sum_avg_gpa,
        RANK() OVER (PARTITION BY n_rows, distinct_ids ORDER BY n_rows DESC, is_international, enrolled_on_ym) AS n_rows_rank
    FROM stats
),
leaders AS (
    -- Top 2 per group
    SELECT
        is_international,
        enrolled_on_ym,
        distinct_ids,
        n_rows,
        n_rows_sum_avg_gpa
    FROM windowed
    WHERE n_rows_rank <= 2
)
-- Subtotals with GROUPING SETS
SELECT
    n_rows,
    distinct_ids,
    PERCENTILE_APPROX(n_rows_sum_avg_gpa, 0.5) AS median_n_rows_sum_avg_gpa,
    MAX(n_rows_sum_avg_gpa) AS max_n_rows_sum_avg_gpa
FROM leaders
GROUP BY GROUPING SETS ((n_rows, distinct_ids), (n_rows), (distinct_ids))
ORDER BY n_rows NULLS LAST, distinct_ids NULLS LAST
