WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('FAIL', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 09:00:00'
      AND grade_status IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        term_group,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM recent
    WHERE grade_status IN ('WITHDRAWN', 'PASS')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(enrolled_at_year, 0) AS enrolled_at_year_filled,
        HOUR(enrolled_at) AS enrolled_at_hour,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM derived
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        grade_status,
        term_group,
        enrolled_at_year,
        enrolled_at_plus_30m,
        LAG(enrolled_at_plus_30m) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at_plus_30m,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY enrolled_at_hour)) AS distinct_grade_status_in_enrolled_at_hour,
        NTILE(5) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS enrolled_at_quintile,
        enrolled_at_hour - LAG(enrolled_at_hour) OVER (ORDER BY enrolled_at_plus_30m DESC, id) AS enrolled_at_hour_change
    FROM labeled
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE enrolled_at_quintile < (SELECT MAX(enrolled_at_quintile) FROM sequenced)
)
-- Final projection
SELECT
    id,
    term,
    distinct_grade_status_in_enrolled_at_hour,
    enrolled_at_hour_change,
    COALESCE(enrolled_at_year, 0) AS enrolled_at_year_filled
FROM filtered
