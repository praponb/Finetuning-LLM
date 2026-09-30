WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS row_seq,
        STDDEV_SAMP(amount) OVER (PARTITION BY order_id) AS order_id_stddev_samp_amount
    FROM payments
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    order_id,
    SIZE(COLLECT_SET(method)) AS num_methods,
    SIZE(COLLECT_SET(method)) AS num_methods_2,
    COUNT_IF(status = 'REFUNDED') AS refunded_events
FROM scored
GROUP BY DATE_TRUNC('YEAR', paid_at), order_id
ORDER BY num_methods DESC;
