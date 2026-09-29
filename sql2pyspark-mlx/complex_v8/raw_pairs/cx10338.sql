WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE end_date >= DATE '2026-02-16'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, holder_id), 2) AS premium_vs_status_holder_id_avg,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status, holder_id)) AS distinct_product_in_status_holder_id,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        DENSE_RANK() OVER w_status_by_end_date_ties AS end_date_dense_rank,
        premium - LAG(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC NULLS FIRST, id) AS premium_change,
        ROUND(AVG(premium) OVER (PARTITION BY status, YEAR(end_date) ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        NTILE(10) OVER (PARTITION BY status ORDER BY end_date, id) AS end_date_decile,
        MAX(premium) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        SUM(premium) OVER (PARTITION BY status, YEAR(end_date) ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium DESC NULLS FIRST, id) AS seq_no,
        LAST_VALUE(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        COUNT(*) OVER w_status_holder_id AS rows_in_status_holder_id
    FROM base_policys
    WINDOW w_status_by_end_date_ties AS (PARTITION BY status ORDER BY end_date), w_status_holder_id AS (PARTITION BY status, holder_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_vs_status_holder_id_avg,
        distinct_product_in_status_holder_id,
        second_premium,
        end_date_dense_rank,
        premium_change,
        premium_avg_4,
        end_date_decile,
        max_premium_so_far,
        running_premium,
        last_premium,
        rows_in_status_holder_id
    FROM with_trends
    WHERE seq_no <= 5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        premium_vs_status_holder_id_avg,
        distinct_product_in_status_holder_id,
        second_premium,
        end_date_decile,
        max_premium_so_far,
        last_premium,
        rows_in_status_holder_id,
        SIZE(COLLECT_SET(holder_id) OVER w_end_date_decile) AS distinct_holder_id_in_end_date_decile,
        NTILE(3) OVER w_all_by_end_date_decile AS end_date_decile_tercile,
        LEAD(premium_avg_4) OVER (ORDER BY end_date_decile, id) AS next_premium_avg_4,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), end_date_decile) AS rows_in_end_date_month_end_date_decile,
        DENSE_RANK() OVER w_end_date_year_status_by_prem AS premium_vs_status_holder_id_avg_dense_rank,
        DENSE_RANK() OVER (ORDER BY end_date_decile, id) AS end_date_decile_dense_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY end_date_decile)) AS distinct_status_in_end_date_decile,
        RANK() OVER (ORDER BY premium_change, id) AS premium_change_rank
    FROM top_rows
    WINDOW w_end_date_decile AS (PARTITION BY end_date_decile), w_all_by_end_date_decile AS (ORDER BY end_date_decile, id), w_end_date_year_status_by_prem AS (PARTITION BY YEAR(end_date), status ORDER BY premium_vs_status_holder_id_avg ASC)
)
SELECT
    id,
    product,
    status,
    end_date_decile,
    max_premium_so_far,
    last_premium,
    rows_in_status_holder_id,
    distinct_holder_id_in_end_date_decile,
    end_date_decile_tercile,
    rows_in_end_date_month_end_date_decile
FROM metrics
ORDER BY rows_in_end_date_month_end_date_decile DESC, id DESC
