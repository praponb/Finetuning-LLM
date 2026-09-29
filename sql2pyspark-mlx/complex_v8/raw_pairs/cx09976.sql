WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE grade_status IN ('PASS', 'WITHDRAWN', 'FAIL')
),
grouped AS (
    -- Group-level totals
    SELECT
        grade_status,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        DATE_TRUNC('MONTH', enrolled_at_quarter) AS enrolled_at_quarter_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'WITHDRAWN' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_withdrawn,
        SUM(CASE WHEN term IS NOT NULL THEN 1 ELSE 0 END) AS n_term_is_not_null
    FROM prepared_enrollments
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(pct_grade_status_withdrawn) OVER (PARTITION BY row_count, grade_status), 2) AS row_count_grade_status_avg_pct_grade_status_with,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY row_count, grade_status), 2) AS row_count_grade_status_avg_distinct_ids,
        COUNT(*) OVER (PARTITION BY row_count, grade_status) AS rows_in_row_count_grade_status,
        ROUND(n_term_is_not_null / NULLIF(SUM(n_term_is_not_null) OVER (PARTITION BY row_count, grade_status), 0), 4) AS n_term_is_not_null_share_of_row_count_grade_stat,
        MAX(distinct_ids) OVER (PARTITION BY row_count, grade_status) AS row_count_grade_status_max_distinct_ids
    FROM grouped
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY distinct_ids), 2) AS distinct_ids_vs_distinct_ids_avg,
        NTILE(4) OVER w_all_by_n_term_is_not_null AS n_term_is_not_null_quartile,
        FIRST_VALUE(enrolled_at_quarter_month) OVER (ORDER BY n_term_is_not_null ASC NULLS LAST, grade_status, enrolled_at_month, enrolled_at_quarter_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_quarter_month
    FROM with_windows
    WINDOW w_all_by_n_term_is_not_null AS (ORDER BY n_term_is_not_null ASC, grade_status, enrolled_at_month, enrolled_at_quarter_month)
)
-- Native SQL UNPIVOT (wide to long)
SELECT grade_status, enrolled_at_month, field, value
FROM (
    SELECT grade_status, enrolled_at_month, n_term_is_not_null_share_of_row_count_grade_stat, row_count_grade_status_avg_pct_grade_status_with, row_count_grade_status_avg_distinct_ids
    FROM with_windows_2
) src
UNPIVOT (
    value FOR field IN (n_term_is_not_null_share_of_row_count_grade_stat AS m_n_term_is_not_null_share_of_row_count_grade_st, row_count_grade_status_avg_pct_grade_status_with AS row_count_grade_status_avg_pct_grade_status_with_2, row_count_grade_status_avg_distinct_ids AS m_row_count_grade_status_avg_distinct_ids)
) unp
ORDER BY grade_status, enrolled_at_month, field
