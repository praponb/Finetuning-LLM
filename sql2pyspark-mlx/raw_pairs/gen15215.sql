WITH base AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LENGTH(status) AS status_length
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-26 14:00:00'
      AND method IS NOT NULL
)
SELECT
    method,
    CASE WHEN status_length >= 65 THEN 'HIGH' ELSE 'LOW' END AS status_length_tier,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    COUNT(*) AS num_rows,
    MIN(paid_at) AS started_at
FROM base
GROUP BY method, CASE WHEN status_length >= 65 THEN 'HIGH' ELSE 'LOW' END
ORDER BY duration_min;
