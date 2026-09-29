WITH cleaned AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'MMM yyyy') AS opened_on_label,
        DATEDIFF(DATE '2026-01-01', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(days_since_opened_on) OVER (ORDER BY capacity DESC, id) AS next_days_since_opened_on,
        COUNT(*) OVER (PARTITION BY MONTH(opened_on)) AS rows_in_opened_on_month,
        MAX(capacity) OVER (PARTITION BY MONTH(opened_on)) AS opened_on_month_max_capacity,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on)) AS rows_in_opened_on_month_2,
        ROW_NUMBER() OVER (ORDER BY capacity ASC, id) AS capacity_seq,
        ROUND(AVG(capacity) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY capacity DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS capacity_avg_1_prior,
        LEAD(days_since_opened_on) OVER (ORDER BY days_since_opened_on NULLS LAST, id) AS next_days_since_opened_on_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY capacity DESC, id), 3) AS capacity_percent_rank,
        MAX(days_since_opened_on) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on)) AS opened_on_month_max_days_since_opened_on,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_on) ORDER BY capacity DESC, id) AS rn,
        ROW_NUMBER() OVER w_opened_on_month_by_opened_on_2 AS rn_2,
        NTILE(5) OVER (ORDER BY capacity ASC, id) AS capacity_quintile
    FROM cleaned
    WINDOW w_opened_on_month_by_opened_on_2 AS (PARTITION BY MONTH(opened_on) ORDER BY opened_on DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        days_since_opened_on,
        next_days_since_opened_on,
        rows_in_opened_on_month,
        capacity_avg_1_prior,
        capacity_percent_rank,
        opened_on_month_max_days_since_opened_on,
        LAST_VALUE(next_days_since_opened_on_2) OVER (ORDER BY capacity_avg_1_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_days_since_opened_on_2,
        LAG(opened_on, 3) OVER w_capacity_quintile_rn_2_by_ro AS prev3_opened_on,
        ROUND(capacity_percent_rank - AVG(capacity_percent_rank) OVER (PARTITION BY capacity_seq), 2) AS capacity_percent_rank_vs_capacity_seq_avg,
        MIN(rows_in_opened_on_month) OVER w_capacity_seq AS capacity_seq_min_rows_in_opened_on_month,
        ROW_NUMBER() OVER w_capacity_quintile_rn_2_by_ro AS rn_3,
        capacity_avg_1_prior - LAG(capacity_avg_1_prior) OVER (PARTITION BY capacity_quintile, rn_2 ORDER BY rows_in_opened_on_month ASC, id) AS capacity_avg_1_prior_change,
        NTH_VALUE(capacity_avg_1_prior, 2) OVER (PARTITION BY capacity_quintile, rn_2 ORDER BY rows_in_opened_on_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity_avg_1_prior
    FROM sequenced
    WINDOW w_capacity_quintile_rn_2_by_ro AS (PARTITION BY capacity_quintile, rn_2 ORDER BY rows_in_opened_on_month ASC, id), w_capacity_seq AS (PARTITION BY capacity_seq)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        DATE_FORMAT(prev3_opened_on, 'yyyy-MM') AS prev3_opened_on_ym,
        rn_3,
        SUM(CASE WHEN wh_code IS NOT NULL THEN 1 ELSE 0 END) AS n_wh_code_is_not_null,
        COUNT(*) AS cnt,
        ROUND(AVG(capacity), 2) AS avg_capacity,
        SUM(CASE WHEN LENGTH(TRIM(wh_code)) >= 7 THEN capacity_percent_rank_vs_capacity_seq_avg ELSE 0 END) AS capacity_percent_rank_vs_capacity_seq_avg_length
    FROM metrics
    WHERE last_next_days_since_opened_on_2 <= 455
    GROUP BY DATE_TRUNC('MONTH', opened_on), DATE_FORMAT(prev3_opened_on, 'yyyy-MM'), rn_3
    HAVING COUNT(*) >= 3
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        opened_on_month,
        prev3_opened_on_ym,
        rn_3,
        cnt,
        avg_capacity,
        SUM(avg_capacity) OVER (PARTITION BY n_wh_code_is_not_null, rn_3 ORDER BY cnt, opened_on_month, prev3_opened_on_ym, rn_3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_capacity_sum_3,
        ROW_NUMBER() OVER w_all_by_n_wh_code_is_not_null AS rn,
        NTILE(5) OVER (ORDER BY n_wh_code_is_not_null DESC, opened_on_month, prev3_opened_on_ym, rn_3) AS n_wh_code_is_not_null_quintile,
        FIRST_VALUE(n_wh_code_is_not_null) IGNORE NULLS OVER (ORDER BY n_wh_code_is_not_null DESC, opened_on_month, prev3_opened_on_ym, rn_3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_wh_code_is_not_null
    FROM rollup_base
    WINDOW w_all_by_n_wh_code_is_not_null AS (ORDER BY n_wh_code_is_not_null DESC, opened_on_month, prev3_opened_on_ym, rn_3)
)
SELECT
    opened_on_month,
    prev3_opened_on_ym,
    rn_3,
    avg_capacity_sum_3,
    n_wh_code_is_not_null_quintile,
    LOWER(REGEXP_REPLACE(TRIM(prev3_opened_on_ym), ' ', '_')) AS prev3_opened_on_ym_slug,
    ABS(cnt - 5) AS cnt_distance
FROM metrics_2
ORDER BY opened_on_month DESC, prev3_opened_on_ym, rn_3
LIMIT 10
