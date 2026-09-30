WITH cleaned_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag,
        QUARTER(enrolled_on) AS enrolled_on_quarter
    FROM students
    WHERE LENGTH(TRIM(student_name)) >= 7
      AND gpa < 3.4
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        major,
        is_international_flag,
        enrolled_on_quarter,
        MIN(is_international_flag) OVER (PARTITION BY major, enrolled_on_quarter) AS major_enrolled_on_quarter_min_is_international_f,
        ROUND(AVG(enrolled_on_quarter) OVER (PARTITION BY major ORDER BY enrolled_on_quarter, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS enrolled_on_quarter_avg_7,
        MAX(is_international_flag) OVER (PARTITION BY is_international_flag, major ORDER BY enrolled_on DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_international_flag_max_3,
        ROW_NUMBER() OVER (PARTITION BY is_international_flag, major ORDER BY enrolled_on DESC NULLS FIRST, id) AS rn,
        ROW_NUMBER() OVER w_enrolled_on_month_enrolled_o AS rn_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrolled_on), enrolled_on_quarter ORDER BY enrolled_on, id), 3) AS enrolled_on_percent_rank
    FROM cleaned_students
    WINDOW w_enrolled_on_month_enrolled_o AS (PARTITION BY MONTH(enrolled_on), enrolled_on_quarter ORDER BY enrolled_on, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(enrolled_on_quarter_avg_7 / NULLIF(SUM(enrolled_on_quarter_avg_7) OVER (PARTITION BY major_enrolled_on_quarter_min_is_international_f, is_international_flag), 0), 4) AS enrolled_on_quarter_avg_7_share_of_major_enrolle,
        ROUND(AVG(enrolled_on_quarter_avg_7) OVER w_major_enrolled_on_quarter_mi, 2) AS major_enrolled_on_quarter_min_is_international_f_2,
        ROW_NUMBER() OVER w_is_international_flag_max_3 AS seq_no,
        DENSE_RANK() OVER w_rn_is_international_flag_max AS is_international_flag_dense_rank,
        COUNT(*) OVER w_major_enrolled_on_quarter_mi AS rows_in_major_enrolled_on_quarter_min_is_interna
    FROM metrics
    WINDOW w_major_enrolled_on_quarter_mi AS (PARTITION BY major_enrolled_on_quarter_min_is_international_f, is_international_flag), w_is_international_flag_max_3 AS (PARTITION BY is_international_flag_max_3 ORDER BY rn NULLS LAST, id), w_rn_is_international_flag_max AS (PARTITION BY rn, is_international_flag_max_3 ORDER BY is_international_flag)
)
-- Final projection
SELECT
    id,
    is_international_flag,
    enrolled_on_quarter,
    major_enrolled_on_quarter_min_is_international_f,
    is_international_flag_max_3,
    rn,
    rn_2,
    enrolled_on_percent_rank,
    enrolled_on_quarter_avg_7_share_of_major_enrolle,
    major_enrolled_on_quarter_min_is_international_f_2,
    seq_no,
    is_international_flag_dense_rank,
    rows_in_major_enrolled_on_quarter_min_is_interna,
    rn - rn_2 AS rn_minus_rn_2,
    ROUND(enrolled_on_percent_rank * 1.07, 2) AS enrolled_on_percent_rank_adj
FROM windowed
ORDER BY major_enrolled_on_quarter_min_is_international_f_2 DESC, rn_minus_rn_2 DESC NULLS FIRST, id
LIMIT 5
