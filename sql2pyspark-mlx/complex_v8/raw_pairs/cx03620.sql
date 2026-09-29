WITH valid AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        ADD_MONTHS(enrolled_on, 1) AS enrolled_on_plus_1m
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'G%'
      AND cohort <> 'C2024'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        enrolled_on_plus_1m,
        COUNT(*) OVER (PARTITION BY cohort) AS rows_in_cohort,
        ROUND(AVG(gpa) OVER (PARTITION BY cohort), 2) AS cohort_avg_gpa,
        ROUND(gpa - AVG(gpa) OVER w_cohort, 2) AS gpa_vs_cohort_avg,
        SUM(gpa) OVER (PARTITION BY cohort) AS cohort_sum_gpa
    FROM valid
    WINDOW w_cohort AS (PARTITION BY cohort)
),
cohort_stats AS (
    -- Avg rows_in_cohort per cohort
    SELECT cohort, AVG(rows_in_cohort) AS avg_rows_in_cohort
    FROM sequenced
    GROUP BY cohort
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rows_in_cohort,
        ROUND(s.rows_in_cohort - g.avg_rows_in_cohort, 2) AS rows_in_cohort_minus_avg
    FROM sequenced s
    JOIN cohort_stats g ON g.cohort = s.cohort
    WHERE s.rows_in_cohort > g.avg_rows_in_cohort
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM compared
    WHERE rows_in_cohort > (SELECT MIN(rows_in_cohort) FROM compared)
)
SELECT
    id,
    student_name,
    cohort,
    rows_in_cohort,
    cohort_avg_gpa,
    avg_rows_in_cohort,
    rows_in_cohort_minus_avg
FROM filtered
WHERE rows_in_cohort >= 5
