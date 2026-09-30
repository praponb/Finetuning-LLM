WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND student_id >= 9
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status = 'FAIL'
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        term,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        MAX(enrolled_at) AS last_enrolled_at
    FROM screened
    GROUP BY 1, 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        enrolled_at_month,
        term,
        first_enrolled_at,
        n_rows,
        last_enrolled_at,
        ROUND(n_rows - AVG(n_rows) OVER (PARTITION BY distinct_ids), 2) AS n_rows_vs_distinct_ids_avg,
        MAX(distinct_ids) OVER (PARTITION BY YEAR(last_enrolled_at) ORDER BY n_rows, enrolled_at_month, term ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS distinct_ids_max_2,
        MIN(n_rows) OVER (PARTITION BY YEAR(last_enrolled_at) ORDER BY n_rows, enrolled_at_month, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_n_rows_so_far,
        NTILE(10) OVER (PARTITION BY YEAR(last_enrolled_at) ORDER BY n_rows, enrolled_at_month, term) AS n_rows_decile,
        RANK() OVER (PARTITION BY YEAR(last_enrolled_at) ORDER BY n_rows) AS n_rows_rank,
        LAG(last_enrolled_at, 3) OVER w_last_enrolled_at_year_by_n_r_3 AS prev3_last_enrolled_at
    FROM grouped
    WINDOW w_last_enrolled_at_year_by_n_r_3 AS (PARTITION BY YEAR(last_enrolled_at) ORDER BY n_rows, enrolled_at_month, term)
),
scored AS (
    -- Derive helper columns
    SELECT
        enrolled_at_month,
        term,
        first_enrolled_at,
        last_enrolled_at,
        n_rows_vs_distinct_ids_avg,
        distinct_ids_max_2,
        min_n_rows_so_far,
        n_rows_rank,
        prev3_last_enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CONCAT(term, '#', CAST(distinct_ids_max_2 AS STRING)) AS term_ref,
        TO_DATE(first_enrolled_at) AS first_enrolled_at_date
    FROM with_windows
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE n_rows_rank > (SELECT AVG(n_rows_rank) FROM scored)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(n_rows_vs_distinct_ids_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', first_enrolled_at_date), MONTH(enrolled_at_month) ORDER BY n_rows_vs_distinct_ids_avg DESC, enrolled_at_month, term ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS n_rows_vs_distinct_ids_avg_max_7,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', first_enrolled_at_date), MONTH(enrolled_at_month) ORDER BY n_rows_vs_distinct_ids_avg DESC), 3) AS n_rows_vs_distinct_ids_avg_cume_dist,
        ROUND((UNIX_TIMESTAMP(first_enrolled_at) - UNIX_TIMESTAMP(LAG(first_enrolled_at) OVER w_first_enrolled_at_date_month_3)) / 60, 1) AS minutes_since_prev_first_enrolled_at,
        NTILE(10) OVER (ORDER BY min_n_rows_so_far, enrolled_at_month, term) AS min_n_rows_so_far_decile
    FROM filtered
    WINDOW w_first_enrolled_at_date_month_3 AS (PARTITION BY DATE_TRUNC('MONTH', first_enrolled_at_date), MONTH(enrolled_at_month) ORDER BY n_rows_vs_distinct_ids_avg DESC, enrolled_at_month, term)
)
SELECT
    enrolled_at_month,
    term,
    last_enrolled_at,
    n_rows_vs_distinct_ids_avg,
    distinct_ids_max_2,
    min_n_rows_so_far,
    prev3_last_enrolled_at,
    term_slug,
    term_ref,
    n_rows_vs_distinct_ids_avg_cume_dist,
    min_n_rows_so_far_decile
FROM metrics
WHERE term_slug IS NOT NULL
ORDER BY term_slug NULLS LAST
