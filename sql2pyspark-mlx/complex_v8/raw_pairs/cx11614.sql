WITH recent_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug
    FROM students
    WHERE gpa <= 3.4
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(gpa) OVER w_enrolled_on_year_is_internat, 2) AS enrolled_on_year_is_international_avg_gpa,
        ROUND(AVG(gpa) OVER (PARTITION BY major ORDER BY enrolled_on, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS gpa_avg_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY is_international, major ORDER BY gpa ASC, id), 3) AS gpa_cume_dist,
        LAG(enrolled_on) OVER (PARTITION BY is_international, major ORDER BY gpa ASC, id) AS prev_enrolled_on,
        ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY enrolled_on DESC, id) AS rn,
        MAX(gpa) OVER (PARTITION BY major ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_gpa_so_far,
        LAST_VALUE(enrolled_on) IGNORE NULLS OVER (PARTITION BY major, YEAR(enrolled_on) ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_on,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_on), is_international) AS rows_in_enrolled_on_year_is_international,
        MAX(gpa) OVER (PARTITION BY major, YEAR(enrolled_on) ORDER BY gpa, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS gpa_max_4
    FROM recent_students
    WINDOW w_enrolled_on_year_is_internat AS (PARTITION BY YEAR(enrolled_on), is_international)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        student_name_slug,
        gpa_avg_1_prior,
        gpa_cume_dist,
        max_gpa_so_far,
        last_known_enrolled_on,
        gpa_max_4,
        DATE_ADD(last_known_enrolled_on, 30) AS last_known_enrolled_on_plus_30d
    FROM with_windows
    WHERE major IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        student_name_slug,
        gpa_cume_dist,
        max_gpa_so_far,
        gpa_max_4,
        ROUND(CUME_DIST() OVER (PARTITION BY major ORDER BY gpa_avg_1_prior), 3) AS gpa_avg_1_prior_cume_dist,
        ROUND(gpa_avg_1_prior - AVG(gpa_avg_1_prior) OVER (PARTITION BY major, DATE_TRUNC('MONTH', last_known_enrolled_on)), 2) AS gpa_avg_1_prior_vs_major_last_known_enrolled_on_,
        ROUND(PERCENT_RANK() OVER w_cohort_by_last_known_enrolle, 3) AS last_known_enrolled_on_plus_30d_percent_rank,
        LAST_VALUE(gpa) OVER (PARTITION BY major ORDER BY gpa_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa,
        ROW_NUMBER() OVER w_major_by_gpa_avg_1_prior AS rn,
        NTILE(4) OVER (PARTITION BY major ORDER BY gpa_avg_1_prior, id) AS gpa_avg_1_prior_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY cohort ORDER BY last_known_enrolled_on_plus_30d DESC), 3) AS last_known_enrolled_on_plus_30d_cume_dist,
        NTH_VALUE(gpa_avg_1_prior, 2) OVER (PARTITION BY cohort ORDER BY last_known_enrolled_on_plus_30d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_avg_1_prior
    FROM scored
    WINDOW w_cohort_by_last_known_enrolle AS (PARTITION BY cohort ORDER BY last_known_enrolled_on_plus_30d DESC), w_major_by_gpa_avg_1_prior AS (PARTITION BY major ORDER BY gpa_avg_1_prior, id)
)
-- Final projection
SELECT
    id,
    student_name,
    cohort,
    enrolled_on,
    gpa,
    student_name_slug,
    max_gpa_so_far,
    gpa_avg_1_prior_cume_dist,
    gpa_avg_1_prior_vs_major_last_known_enrolled_on_,
    last_known_enrolled_on_plus_30d_percent_rank,
    rn,
    second_gpa_avg_1_prior
FROM with_trends
WHERE gpa_cume_dist <= 0.8
