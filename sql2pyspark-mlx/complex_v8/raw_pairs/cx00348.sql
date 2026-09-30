WITH recent AS (
    -- Filter students
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        is_international,
        CONCAT(student_name, '#', CAST(id AS STRING)) AS student_name_ref
    FROM students
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY major ORDER BY enrolled_on DESC NULLS FIRST) AS enrolled_on_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY major, is_international ORDER BY enrolled_on ASC, id) AS rn,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_on ASC), 3) AS enrolled_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY is_international ORDER BY enrolled_on ASC, id) AS enrolled_on_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrolled_on) ORDER BY enrolled_on DESC), 3) AS enrolled_on_percent_rank,
        FIRST_VALUE(enrolled_on) IGNORE NULLS OVER (PARTITION BY is_international ORDER BY enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_on,
        RANK() OVER (PARTITION BY major ORDER BY enrolled_on ASC, id) AS enrolled_on_rank,
        ROUND(PERCENT_RANK() OVER w_major_is_international_by_en_2, 3) AS enrolled_on_percent_rank_2,
        NTH_VALUE(enrolled_on, 2) OVER (PARTITION BY is_international ORDER BY enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on,
        NTH_VALUE(enrolled_on, 2) OVER (PARTITION BY MONTH(enrolled_on) ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on_2,
        DATEDIFF(enrolled_on, LAG(enrolled_on) OVER (PARTITION BY is_international ORDER BY enrolled_on ASC, id)) AS days_since_prev_enrolled_on,
        ROW_NUMBER() OVER (PARTITION BY major ORDER BY enrolled_on ASC, id) AS enrolled_on_seq_2
    FROM recent
    WINDOW w_major_is_international_by_en_2 AS (PARTITION BY major, is_international ORDER BY enrolled_on ASC)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        is_international,
        student_name_ref,
        enrolled_on_dense_rank,
        enrolled_on_cume_dist,
        enrolled_on_seq,
        enrolled_on_percent_rank,
        enrolled_on_percent_rank_2,
        second_enrolled_on,
        second_enrolled_on_2,
        days_since_prev_enrolled_on,
        enrolled_on_seq_2,
        IF(is_international, 'yes', 'no') AS is_international_label,
        QUARTER(second_enrolled_on_2) AS second_enrolled_on_2_quarter
    FROM sequenced
    WHERE enrolled_on >= DATE '2024-04-01'
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(enrolled_on_percent_rank) OVER w_is_international_label, 2) AS is_international_label_avg_enrolled_on_percent_r,
        ROUND(second_enrolled_on_2_quarter / NULLIF(SUM(second_enrolled_on_2_quarter) OVER (PARTITION BY is_international_label), 0), 4) AS second_enrolled_on_2_quarter_share_of_is_interna,
        MIN(enrolled_on_dense_rank) OVER (PARTITION BY is_international_label) AS is_international_label_min_enrolled_on_dense_ran,
        DATEDIFF(second_enrolled_on, LAG(second_enrolled_on) OVER w_enrolled_on_dense_rank_by_en) AS days_since_prev_second_enrolled_on,
        SIZE(COLLECT_SET(major) OVER (PARTITION BY is_international, enrolled_on_dense_rank)) AS distinct_major_in_is_international_enrolled_on_d,
        SIZE(COLLECT_SET(is_international_label) OVER (PARTITION BY is_international, enrolled_on_dense_rank)) AS distinct_is_international_label_in_is_internatio
    FROM scored
    WINDOW w_is_international_label AS (PARTITION BY is_international_label), w_enrolled_on_dense_rank_by_en AS (PARTITION BY enrolled_on_dense_rank ORDER BY enrolled_on_percent_rank DESC, id)
)
-- Final projection
SELECT
    id,
    student_name,
    enrolled_on,
    is_international,
    enrolled_on_dense_rank,
    enrolled_on_cume_dist,
    enrolled_on_seq,
    enrolled_on_percent_rank,
    enrolled_on_percent_rank_2,
    enrolled_on_seq_2,
    is_international_label,
    second_enrolled_on_2_quarter,
    is_international_label_avg_enrolled_on_percent_r,
    second_enrolled_on_2_quarter_share_of_is_interna,
    is_international_label_min_enrolled_on_dense_ran,
    days_since_prev_second_enrolled_on,
    distinct_major_in_is_international_enrolled_on_d,
    distinct_is_international_label_in_is_internatio
FROM sequenced_2
ORDER BY distinct_is_international_label_in_is_internatio DESC
