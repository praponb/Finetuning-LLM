WITH valid_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CONCAT_WS(': ', major, student_name) AS major_student_name_label
    FROM students
    WHERE cohort <> 'C2023'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        is_international,
        major_student_name_label,
        LAG(enrolled_on, 2) OVER (PARTITION BY major, cohort ORDER BY gpa ASC, id) AS prev2_enrolled_on,
        ROUND(AVG(gpa) OVER (PARTITION BY major, is_international ORDER BY gpa DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3,
        FIRST_VALUE(gpa) IGNORE NULLS OVER (PARTITION BY major, is_international ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_gpa,
        SUM(gpa) OVER (PARTITION BY major, cohort ORDER BY gpa ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS gpa_sum_2_prior,
        SUM(gpa) OVER (PARTITION BY major, cohort ORDER BY gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        ROUND(AVG(gpa) OVER (PARTITION BY major ORDER BY enrolled_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3_2,
        SIZE(COLLECT_SET(major) OVER (PARTITION BY is_international, cohort)) AS distinct_major_in_is_international_cohort,
        LAST_VALUE(enrolled_on) OVER (PARTITION BY major ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_on
    FROM valid_students
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', last_enrolled_on) AS days_since_last_enrolled_on
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(first_known_gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', last_enrolled_on), major ORDER BY days_since_last_enrolled_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS first_known_gpa_avg_7,
        DENSE_RANK() OVER w_major_distinct_major_in_is_i AS gpa_sum_2_prior_dense_rank,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', last_enrolled_on), major ORDER BY days_since_last_enrolled_on, id) AS days_since_last_enrolled_on_decile,
        COUNT(*) OVER (PARTITION BY is_international) AS rows_in_is_international,
        NTH_VALUE(gpa_avg_3_2, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', last_enrolled_on), major ORDER BY days_since_last_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_avg_3_2
    FROM derived
    WINDOW w_major_distinct_major_in_is_i AS (PARTITION BY major, distinct_major_in_is_international_cohort ORDER BY gpa_sum_2_prior ASC)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', last_enrolled_on) AS last_enrolled_on_month,
        cohort,
        SUM(CASE WHEN UPPER(TRIM(student_name)) LIKE 'J%' THEN 1 ELSE 0 END) AS n_upper_trim_student_name_like,
        ROUND(100 * COUNT(CASE WHEN cohort = 'C2021' THEN 1 END) / COUNT(*), 1) AS pct_cohort_c2021
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', last_enrolled_on), cohort
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        last_enrolled_on_month,
        cohort,
        n_upper_trim_student_name_like,
        pct_cohort_c2021,
        COUNT(*) OVER (PARTITION BY cohort) AS rows_in_cohort,
        ROUND(PERCENT_RANK() OVER w_all_by_n_upper_trim_student, 3) AS n_upper_trim_student_name_like_percent_rank,
        SUM(pct_cohort_c2021) OVER (ORDER BY n_upper_trim_student_name_like, last_enrolled_on_month, cohort ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pct_cohort_c2021_sum_2_prior
    FROM summary
    WINDOW w_all_by_n_upper_trim_student AS (ORDER BY n_upper_trim_student_name_like, last_enrolled_on_month, cohort)
)
SELECT
    last_enrolled_on_month,
    cohort,
    n_upper_trim_student_name_like,
    pct_cohort_c2021,
    rows_in_cohort,
    n_upper_trim_student_name_like_percent_rank,
    pct_cohort_c2021_sum_2_prior,
    DATE_FORMAT(last_enrolled_on_month, 'yyyy-MM-dd HH:00') AS last_enrolled_on_month_label,
    TO_DATE(last_enrolled_on_month) AS last_enrolled_on_month_date
FROM ranked_2
ORDER BY last_enrolled_on_month_label, rows_in_cohort, last_enrolled_on_month, cohort
LIMIT 20
