WITH base AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international
    FROM students
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag,
        DATE_ADD(enrolled_on, 90) AS enrolled_on_plus_90d,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM base
    WHERE NOT is_international
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE is_international_flag > (SELECT AVG(is_international_flag) FROM labeled)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(enrolled_on) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_on), is_international_flag ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_on,
        NTH_VALUE(enrolled_on, 2) OVER (PARTITION BY MONTH(enrolled_on), is_international_flag ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on,
        MIN(is_international_flag) OVER (PARTITION BY is_international_flag) AS is_international_flag_min_is_international_flag,
        ROUND(is_international_flag - AVG(is_international_flag) OVER (PARTITION BY is_international_flag), 2) AS is_international_flag_vs_is_international_flag_a,
        DATEDIFF(enrolled_on_plus_90d, LAG(enrolled_on_plus_90d) OVER (PARTITION BY MONTH(enrolled_on), is_international_flag ORDER BY gpa, id)) AS days_since_prev_enrolled_on_plus_90d,
        ROUND(AVG(gpa) OVER (PARTITION BY MONTH(enrolled_on), is_international_flag ORDER BY gpa, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS gpa_avg_6_prior
    FROM without_matches
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, is_international_flag, is_international_flag_min_is_international_flag
    FROM sequenced
) src
UNPIVOT (
    value FOR field IN (is_international_flag, is_international_flag_min_is_international_flag)
) unp
ORDER BY id, field
