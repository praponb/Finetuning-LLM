WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(premium) OVER (ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date))) AS distinct_status_in_start_date_month,
        COUNT(*) OVER (PARTITION BY status, product_len) AS rows_in_status_product_len,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS rows_in_start_date_month,
        ROUND(product_len / NULLIF(SUM(product_len) OVER w_status_product_len, 0), 4) AS product_len_share_of_status_product_len,
        SIZE(COLLECT_SET(holder_id) OVER w_status_product_len) AS distinct_holder_id_in_status_product_len,
        ROUND(premium_adj - AVG(premium_adj) OVER (PARTITION BY status, product_len), 2) AS premium_adj_vs_status_product_len_avg,
        SUM(premium_adj) OVER (PARTITION BY status, product_len ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_adj_sum_4
    FROM scoped
    WINDOW w_status_product_len AS (PARTITION BY status, product_len)
)
SELECT
    id,
    premium,
    start_date,
    product_len,
    premium_adj,
    premium_avg_6_prior,
    distinct_status_in_start_date_month,
    rows_in_start_date_month,
    product_len_share_of_status_product_len,
    distinct_holder_id_in_status_product_len,
    premium_adj_sum_4
FROM metrics
ORDER BY premium_adj DESC, product_len DESC
