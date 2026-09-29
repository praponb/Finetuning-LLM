WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        CASE WHEN score >= 79.2 THEN 'large' WHEN score >= 38.5 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE term IN ('2024T1', '2024T2')
      AND score <= 85.8
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE enrolled_at_dayofweek > (SELECT AVG(enrolled_at_dayofweek) FROM cleaned)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_ref,
        enrolled_at_dayofweek,
        score_tier,
        ROW_NUMBER() OVER (PARTITION BY term, student_id ORDER BY enrolled_at_dayofweek ASC, id) AS rn,
        COUNT(*) OVER w_enrolled_at_year AS rows_in_enrolled_at_year
    FROM qualified
    WINDOW w_enrolled_at_year AS (PARTITION BY YEAR(enrolled_at))
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(score * 0.9, 2) AS score_adj,
        SUBSTRING(TRIM(term_ref), 1, 3) AS term_ref_prefix,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM metrics
    WHERE rn >= 5
),
summary AS (
    -- Summarise per group
    SELECT
        enrolled_at_dayofweek,
        MIN(rows_in_enrolled_at_year) AS min_rows_in_enrolled_at_year,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MIN(rn) AS min_rn,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(*) AS row_count,
        SUM(enrolled_at_hour) AS total_enrolled_at_hour
    FROM tagged
    GROUP BY enrolled_at_dayofweek
)
-- Subtotals with ROLLUP
SELECT
    distinct_student_ids,
    enrolled_at_dayofweek,
    min_rn,
    ROUND(STDDEV(row_count), 2) AS stddev_row_count
FROM summary
GROUP BY ROLLUP(distinct_student_ids, enrolled_at_dayofweek, min_rn)
