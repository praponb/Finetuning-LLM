WITH prepared_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        DATEDIFF(DATE '2025-12-31', enrolled_on) AS days_since_enrolled_on
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'A%'
      AND enrolled_on < DATE '2025-07-18'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(days_since_enrolled_on) OVER (ORDER BY days_since_enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_enrolled_on,
        MIN(gpa) OVER (ORDER BY days_since_enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_gpa_so_far,
        MAX(gpa) OVER (ORDER BY days_since_enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_gpa_so_far
    FROM prepared_students
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        enrolled_on_month,
        days_since_enrolled_on,
        min_gpa_so_far,
        max_gpa_so_far,
        MAX(days_since_enrolled_on) OVER (ORDER BY gpa, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_enrolled_on_max_1_prior,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_on_month), cohort) AS rows_in_enrolled_on_month_month_cohort,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY MONTH(enrolled_on_month), cohort), 0), 4) AS gpa_share_of_enrolled_on_month_month_cohort,
        LEAD(gpa) OVER w_is_international_by_gpa AS next_gpa,
        NTH_VALUE(gpa, 2) OVER (ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa
    FROM ranked
    WINDOW w_is_international_by_gpa AS (PARTITION BY is_international ORDER BY gpa DESC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        is_international,
        enrolled_on_month,
        days_since_enrolled_on,
        min_gpa_so_far,
        max_gpa_so_far,
        days_since_enrolled_on_max_1_prior,
        rows_in_enrolled_on_month_month_cohort,
        next_gpa,
        second_gpa,
        ROUND(min_gpa_so_far - AVG(min_gpa_so_far) OVER w_is_international, 2) AS min_gpa_so_far_vs_is_international_avg,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY is_international), 0), 4) AS gpa_share_of_is_international
    FROM ranked_2
    WINDOW w_is_international AS (PARTITION BY is_international)
)
SELECT id, metric, amount
FROM (
    SELECT id, days_since_enrolled_on_max_1_prior, days_since_enrolled_on
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (days_since_enrolled_on_max_1_prior, days_since_enrolled_on)
) unp
ORDER BY id, metric
