WITH with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        MIN(amount) OVER (PARTITION BY order_id) AS order_id_min_amount,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY amount NULLS LAST) AS amount_cume_dist,
        PERCENT_RANK() OVER (ORDER BY amount DESC NULLS LAST) AS amount_percent_rank,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount_avg_6
    FROM payments
)
SELECT
    method,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN amount >= 1650 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    ROUND(SUM(amount_percent_rank) / NULLIF(COUNT(CASE WHEN method = 'paypal' THEN 1 END), 0), 2) AS amount_percent_rank_per_paypal
FROM with_windows
GROUP BY method, DATE_TRUNC('WEEK', paid_at), CASE WHEN amount >= 1650 THEN 'HIGH' ELSE 'LOW' END
ORDER BY method DESC NULLS LAST
