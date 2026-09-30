WITH filtered_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international,
        QUARTER(enrolled_on) AS enrolled_on_quarter
    FROM students
    WHERE enrolled_on >= DATE '2024-04-05'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(gpa) OVER (PARTITION BY is_international) AS is_international_min_gpa,
        ROUND(enrolled_on_quarter - AVG(enrolled_on_quarter) OVER (PARTITION BY is_international), 2) AS enrolled_on_quarter_vs_is_international_avg,
        RANK() OVER (PARTITION BY YEAR(enrolled_on), is_international ORDER BY gpa DESC) AS gpa_rank
    FROM filtered_students
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_on_quarter,
        is_international,
        YEAR(enrolled_on) AS enrolled_on_year,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
        COUNT(*) AS row_count,
        SUM(gpa_rank) AS total_gpa_rank
    FROM sequenced
    WHERE major <> 'Art'
    GROUP BY enrolled_on_quarter, is_international, YEAR(enrolled_on)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(row_count, 2) OVER (ORDER BY total_gpa_rank, enrolled_on_quarter, is_international, enrolled_on_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        ROUND(CUME_DIST() OVER (PARTITION BY enrolled_on_quarter, row_count ORDER BY row_count DESC), 3) AS row_count_cume_dist,
        ROW_NUMBER() OVER (ORDER BY total_gpa_rank, enrolled_on_quarter, is_international, enrolled_on_year) AS total_gpa_rank_seq,
        ROUND(enrolled_on_year / NULLIF(SUM(enrolled_on_year) OVER w_row_count, 0), 4) AS enrolled_on_year_share_of_row_count,
        MIN(enrolled_on_quarter) OVER (PARTITION BY row_count) AS row_count_min_enrolled_on_quarter,
        COUNT(*) OVER (PARTITION BY row_count) AS rows_in_row_count
    FROM summary
    WINDOW w_row_count AS (PARTITION BY row_count)
),
flattened AS (
    -- One row per student_name_list_item
    SELECT
        s.enrolled_on_quarter,
        s.is_international,
        s.enrolled_on_year,
        s.row_count,
        s.total_gpa_rank,
        s.second_row_count,
        s.row_count_cume_dist,
        s.total_gpa_rank_seq,
        s.enrolled_on_year_share_of_row_count,
        s.row_count_min_enrolled_on_quarter,
        s.rows_in_row_count,
        t.student_name_list_item
    FROM sequenced_2 s
    LATERAL VIEW OUTER EXPLODE(s.student_name_list) t AS student_name_list_item
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM flattened
    WHERE row_count_min_enrolled_on_quarter > (SELECT AVG(row_count_min_enrolled_on_quarter) FROM flattened)
),
qualified_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM qualified
    WHERE second_row_count > (SELECT MIN(second_row_count) FROM qualified)
)
SELECT
    enrolled_on_quarter,
    is_international,
    enrolled_on_year,
    total_gpa_rank,
    second_row_count,
    row_count_cume_dist,
    total_gpa_rank_seq,
    enrolled_on_year_share_of_row_count,
    row_count_min_enrolled_on_quarter,
    rows_in_row_count,
    student_name_list_item,
    ROUND(row_count / NULLIF(total_gpa_rank_seq, 0), 3) AS row_count_per_total_gpa_rank_seq,
    enrolled_on_year - total_gpa_rank AS enrolled_on_year_minus_total_gpa_rank
FROM qualified_2
ORDER BY row_count_cume_dist DESC, enrolled_on_quarter DESC NULLS FIRST
