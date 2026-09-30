WITH cleaned_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CONCAT_WS(': ', major, cohort) AS major_cohort_label,
        CASE WHEN cohort IN ('C2023', 'C2021') THEN 'group_a' WHEN cohort = 'C2022' THEN 'group_b' ELSE 'other' END AS cohort_group
    FROM students
    WHERE enrolled_on >= DATE '2024-03-15'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER w_all_by_gpa AS gpa_tercile,
        FIRST_VALUE(gpa) OVER (ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gpa,
        FIRST_VALUE(enrolled_on) OVER (ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_on,
        ROUND(gpa - AVG(gpa) OVER w_cohort, 2) AS gpa_vs_cohort_avg,
        ROUND(AVG(gpa) OVER (ORDER BY enrolled_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY is_international), 2) AS gpa_vs_is_international_avg,
        NTILE(4) OVER w_all_by_gpa AS gpa_quartile,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY cohort), 0), 4) AS gpa_share_of_cohort
    FROM cleaned_students
    WINDOW w_all_by_gpa AS (ORDER BY gpa DESC, id), w_cohort AS (PARTITION BY cohort)
)
SELECT
    id,
    cohort,
    major,
    enrolled_on,
    gpa,
    is_international,
    major_cohort_label,
    cohort_group,
    gpa_tercile,
    first_gpa,
    gpa_vs_cohort_avg,
    gpa_avg_3,
    gpa_quartile
FROM windowed
ORDER BY first_gpa, gpa_vs_cohort_avg DESC
