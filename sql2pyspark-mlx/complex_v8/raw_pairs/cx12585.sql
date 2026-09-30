WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        CONCAT_WS(' / ', status, product) AS status_product_label
    FROM policies
    WHERE product = 'AUTO'
      AND holder_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(status_len, 2) OVER (PARTITION BY status, holder_id ORDER BY status_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        LAG(premium, 1, 0) OVER w_status_holder_id_by_status_l_2 AS prev_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        ROUND(AVG(status_len) OVER (PARTITION BY status, holder_id ORDER BY status_len DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS status_len_avg_3,
        DATEDIFF(end_date, LAG(end_date) OVER w_status_len_by_end_date) AS days_since_prev_end_date
    FROM recent_policys
    WINDOW w_status_holder_id_by_status_l_2 AS (PARTITION BY status, holder_id ORDER BY status_len DESC, id), w_status_len_by_end_date AS (PARTITION BY status_len ORDER BY end_date DESC, id)
)
SELECT *
FROM (
    SELECT holder_id, status, premium
    FROM metrics
) src
PIVOT (
    SUM(premium)
    FOR status IN ('ACTIVE', 'LAPSED', 'EXPIRED')
)
