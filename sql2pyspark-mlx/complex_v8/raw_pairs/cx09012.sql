WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        major,
        enrolled_on,
        gpa
    FROM students
    WHERE enrolled_on BETWEEN DATE '2024-04-04' AND DATE '2025-08-27'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        enrolled_on,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on)), 0), 4) AS gpa_share_of_enrolled_on_month,
        DATEDIFF(enrolled_on, LAG(enrolled_on) OVER w_major_by_gpa) AS days_since_prev_enrolled_on,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on)), 2) AS gpa_vs_enrolled_on_month_avg,
        gpa - LAG(gpa) OVER w_major_by_gpa AS gpa_change,
        gpa - LAG(gpa) OVER (PARTITION BY major ORDER BY enrolled_on DESC NULLS FIRST, id) AS gpa_change_2,
        ROUND(AVG(gpa) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on)), 2) AS enrolled_on_month_avg_gpa
    FROM base_students
    WINDOW w_major_by_gpa AS (PARTITION BY major ORDER BY gpa DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        days_since_prev_enrolled_on,
        gpa_change_2,
        enrolled_on_month_avg_gpa,
        ROUND(PERCENT_RANK() OVER (ORDER BY gpa_share_of_enrolled_on_month DESC, id), 3) AS gpa_share_of_enrolled_on_month_percent_rank,
        SUM(gpa_change_2) OVER (ORDER BY gpa_share_of_enrolled_on_month DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS gpa_change_2_sum_4
    FROM with_trends
)
SELECT id, measure, value
FROM (
    SELECT id, enrolled_on_month_avg_gpa, gpa_change_2
    FROM windowed
) src
UNPIVOT (
    value FOR measure IN (enrolled_on_month_avg_gpa AS enrolled_on_month_avg_gpa_v, gpa_change_2 AS m_gpa_change_2)
) unp
ORDER BY id, measure
