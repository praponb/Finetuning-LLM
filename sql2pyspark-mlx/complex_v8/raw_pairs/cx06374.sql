WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on_2
    FROM students
    WHERE cohort = 'C2021'
      AND UPPER(TRIM(student_name)) LIKE 'D%'
),
stats AS (
    -- Summarise per group
    SELECT
        is_international,
        YEAR(enrolled_on) AS enrolled_on_year,
        DATEDIFF(MAX(enrolled_on), MIN(enrolled_on)) AS enrolled_on_span_days,
        COUNT(*) AS cnt,
        SUM(days_since_enrolled_on_2) AS total_days_since_enrolled_on_2,
        MIN(days_since_enrolled_on) AS min_days_since_enrolled_on
    FROM valid_students
    GROUP BY is_international, YEAR(enrolled_on)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        is_international,
        enrolled_on_year,
        enrolled_on_span_days,
        total_days_since_enrolled_on_2,
        min_days_since_enrolled_on,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY is_international), 2) AS cnt_vs_is_international_avg,
        ROUND(AVG(min_days_since_enrolled_on) OVER w_is_international, 2) AS is_international_avg_min_days_since_enrolled_on,
        min_days_since_enrolled_on - LAG(min_days_since_enrolled_on) OVER (PARTITION BY enrolled_on_year, cnt ORDER BY min_days_since_enrolled_on, is_international, enrolled_on_year) AS min_days_since_enrolled_on_change,
        SUM(enrolled_on_span_days) OVER (PARTITION BY is_international ORDER BY cnt DESC, is_international, enrolled_on_year ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS enrolled_on_span_days_sum_2,
        NTH_VALUE(min_days_since_enrolled_on, 2) OVER (ORDER BY total_days_since_enrolled_on_2 ASC, is_international, enrolled_on_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_days_since_enrolled_on,
        LAG(enrolled_on_span_days, 2) OVER (PARTITION BY enrolled_on_year, cnt ORDER BY min_days_since_enrolled_on, is_international, enrolled_on_year) AS prev2_enrolled_on_span_days
    FROM stats
    WINDOW w_is_international AS (PARTITION BY is_international)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(enrolled_on_span_days, 0) AS enrolled_on_span_days_filled,
        COALESCE(enrolled_on_year, 0) AS enrolled_on_year_filled,
        LEAST(enrolled_on_span_days, enrolled_on_year) AS least_enrolled_on_span_days_enrolled_on_year
    FROM with_trends
    WHERE second_min_days_since_enrolled_on <= 560
)
SELECT
    is_international,
    enrolled_on_year,
    enrolled_on_span_days,
    cnt_vs_is_international_avg,
    min_days_since_enrolled_on_change,
    enrolled_on_span_days_sum_2,
    second_min_days_since_enrolled_on,
    prev2_enrolled_on_span_days,
    enrolled_on_span_days_filled,
    enrolled_on_year_filled,
    least_enrolled_on_span_days_enrolled_on_year,
    enrolled_on_year - enrolled_on_span_days AS enrolled_on_year_minus_enrolled_on_span_days
FROM scored
