WITH base AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end,
        CONCAT_WS('-', major, student_name) AS major_student_name_label,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE (enrolled_on BETWEEN DATE '2024-04-01' AND DATE '2025-06-27' OR major IN ('Math', 'Bio', 'Biz'))
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN gpa >= 3.4 THEN 'large' WHEN gpa >= 2.7 THEN 'medium' ELSE 'small' END AS gpa_tier
    FROM base
    WHERE YEAR(enrolled_on_month_end) = 2024
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_international, 'yes', 'no') AS is_international_label_2,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM derived
    WHERE major_student_name_label IS NOT NULL
),
is_international_label_stats AS (
    -- Max gpa per is_international_label
    SELECT is_international_label, MAX(gpa) AS max_gpa
    FROM derived_2
    GROUP BY is_international_label
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_gpa, ROUND(s.gpa - g.max_gpa, 2) AS gpa_minus_max
    FROM derived_2 s
    JOIN is_international_label_stats g ON g.is_international_label = s.is_international_label
    WHERE s.gpa = g.max_gpa
)
SELECT
    COALESCE(is_international_label_2, 'ALL') AS is_international_label_2,
    COALESCE(cohort, 'ALL') AS cohort,
    COALESCE(gpa_tier, 'ALL') AS gpa_tier,
    ROUND(STDDEV(gpa), 2) AS stddev_gpa,
    SUM(CASE WHEN major IS NOT NULL THEN 1 ELSE 0 END) AS n_major_is_not_null,
    ROUND(SUM(gpa_filled) / NULLIF(SUM(max_gpa), 0), 3) AS gpa_filled_to_max_gpa_ratio
FROM compared
GROUP BY ROLLUP(is_international_label_2, cohort, gpa_tier)
