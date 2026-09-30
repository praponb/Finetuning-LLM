WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE product IS NOT NULL
      AND holder_id <= 17
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        product_clean,
        COUNT(*) OVER w_status_group AS rows_in_status_group,
        ROW_NUMBER() OVER w_all_by_start_date AS rn,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status_group)) AS distinct_holder_id_in_status_group,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date
    FROM cleaned
    WINDOW w_status_group AS (PARTITION BY status_group), w_all_by_start_date AS (ORDER BY start_date DESC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        rn,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        ROUND(STDDEV(premium_avg_1_prior), 2) AS stddev_premium_avg_1_prior,
        COUNT(*) AS row_count
    FROM ranked
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM'), rn, DATE_FORMAT(start_date, 'yyyy-MM')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        end_date_ym,
        rn,
        start_date_ym,
        stddev_premium_avg_1_prior,
        row_count,
        COUNT(*) OVER w_row_count_rn AS rows_in_row_count_rn,
        MAX(rn) OVER w_row_count_rn AS row_count_rn_max_rn,
        ROW_NUMBER() OVER (PARTITION BY rn, row_count ORDER BY row_count NULLS LAST, end_date_ym, rn, start_date_ym) AS rn_2
    FROM stats
    WINDOW w_row_count_rn AS (PARTITION BY row_count, rn)
)
SELECT
    end_date_ym,
    rn,
    start_date_ym,
    stddev_premium_avg_1_prior,
    row_count,
    row_count_rn_max_rn,
    rn_2,
    ROUND(row_count / NULLIF(rn_2, 0), 3) AS row_count_per_rn_2,
    CONCAT(start_date_ym, '#', CAST(rows_in_row_count_rn AS STRING)) AS start_date_ym_ref
FROM windowed
ORDER BY row_count_per_rn_2 DESC, stddev_premium_avg_1_prior DESC
