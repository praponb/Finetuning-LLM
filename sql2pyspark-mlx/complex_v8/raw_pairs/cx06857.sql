WITH base_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE is_international = TRUE
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        is_international,
        is_international_label,
        COUNT(*) OVER (PARTITION BY major, cohort) AS rows_in_major_cohort,
        gpa - LAG(gpa) OVER (PARTITION BY is_international_label, is_international ORDER BY gpa, id) AS gpa_change
    FROM base_students
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        rows_in_major_cohort,
        gpa_change,
        DATE_ADD(enrolled_on, 365) AS enrolled_on_plus_365d
    FROM with_trends
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        major,
        CASE WHEN gpa_change >= 1.6 THEN 'HIGH' WHEN gpa_change >= -0.8 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_change_tier,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(student_name)) LIKE 'G%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_student_name_like,
        SUM(rows_in_major_cohort) AS total_rows_in_major_cohort,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
    FROM annotated
    GROUP BY major, CASE WHEN gpa_change >= 1.6 THEN 'HIGH' WHEN gpa_change >= -0.8 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING SUM(rows_in_major_cohort) > 6
),
enriched AS (
    -- Derive helper columns
    SELECT
        major,
        gpa_change_tier,
        pct_upper_trim_student_name_like,
        student_name_list,
        LENGTH(TRIM(gpa_change_tier)) AS gpa_change_tier_len,
        LOWER(REGEXP_REPLACE(TRIM(gpa_change_tier), ' ', '_')) AS gpa_change_tier_slug
    FROM grouped
    WHERE SIZE(student_name_list) >= 2
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(gpa_change_tier_len, 0) AS gpa_change_tier_len_filled,
        CASE WHEN gpa_change_tier IN ('HIGH', 'MEDIUM') THEN 'group_a' WHEN gpa_change_tier = 'LOW' THEN 'group_b' ELSE 'other' END AS gpa_change_tier_group,
        CASE WHEN gpa_change_tier_len >= 12 THEN 'HIGH' WHEN gpa_change_tier_len >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_change_tier_len_tier
    FROM enriched
    WHERE SIZE(student_name_list) >= 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT major, gpa_change_tier, field, amount
FROM (
    SELECT major, gpa_change_tier, gpa_change_tier_len, gpa_change_tier_len_filled
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (gpa_change_tier_len, gpa_change_tier_len_filled)
) unp
ORDER BY major, gpa_change_tier, field
