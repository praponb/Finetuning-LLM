WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
      AND grade_status IN ('FAIL', 'INCOMPLETE')
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        term_or_unknown,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        INITCAP(TRIM(term_or_unknown)) AS term_or_unknown_clean
    FROM cleaned_enrollments
    WHERE term_or_unknown = '2025T2'
),
scored_2 AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        term_or_unknown,
        enrolled_at_dayofweek,
        term_or_unknown_clean,
        CONCAT(term_or_unknown_clean, '#', CAST(course_id AS STRING)) AS term_or_unknown_clean_ref
    FROM scored
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        SUBSTRING(TRIM(term_or_unknown), 1, 3) AS term_or_unknown_prefix
    FROM scored_2
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 10
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status_group,
        grade_status,
        course_id,
        ROUND(STDDEV(enrolled_at_dayofweek), 2) AS stddev_enrolled_at_dayofweek,
        ROUND(100 * COUNT(CASE WHEN grade_status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_grade_status_is_not_null,
        SUM(enrolled_at_dayofweek) AS total_enrolled_at_dayofweek,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(enrolled_at_dayofweek, 0.5) AS median_enrolled_at_dayofweek
    FROM enriched
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    grade_status_group,
    grade_status,
    course_id,
    stddev_enrolled_at_dayofweek,
    pct_grade_status_is_not_null,
    row_count,
    INITCAP(TRIM(grade_status_group)) AS grade_status_group_clean,
    COALESCE(row_count, 0) AS row_count_filled
FROM summary
ORDER BY grade_status, grade_status_group NULLS LAST
