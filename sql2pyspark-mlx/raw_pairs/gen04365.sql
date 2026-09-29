WITH filtered AS (
    SELECT
        *,
        CONCAT(status, ' ', method) AS status_method,
        TRIM(method) AS clean_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-23 08:00:00'
),
scored AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        status_method,
        AVG(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d,
        MIN(amount) OVER (PARTITION BY method, order_id) AS method_min_amount
    FROM filtered
),
enriched AS (
    SELECT
        id,
        paid_at,
        status,
        method_min_amount,
        AVG(amount_avg_90d) OVER (PARTITION BY status ORDER BY amount_avg_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount_avg_90d,
        SUM(amount_avg_90d) OVER (PARTITION BY status ORDER BY amount_avg_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount_avg_90d,
        MIN(amount_avg_90d) OVER (PARTITION BY status ORDER BY amount_avg_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount_avg_90d,
        COUNT(amount_avg_90d) OVER (PARTITION BY status ORDER BY amount_avg_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount_avg_90d,
        SUM(method_min_amount) OVER (PARTITION BY YEAR(paid_at), order_id ORDER BY method_min_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_method_min_amount
    FROM scored
    WHERE method_min_amount BETWEEN 1262 AND 1549
),
cleaned AS (
    SELECT
        id,
        paid_at,
        status,
        method_min_amount,
        running_avg_amount_avg_90d,
        running_sum_amount_avg_90d,
        running_count_amount_avg_90d,
        running_method_min_amount,
        UPPER(status) AS status_upper
    FROM enriched
    WHERE running_avg_amount_avg_90d >= 680
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    MAX(paid_at) AS last_seen,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MIN(running_count_amount_avg_90d) AS min_running_count_amount_avg_90d,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', paid_at) ORDER BY MIN(running_count_amount_avg_90d) DESC) AS min_running_count_amount_avg_90d_dense_rank
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('QUARTER', paid_at);
