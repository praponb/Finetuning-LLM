SELECT
    CASE WHEN amount >= 1520 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows,
    COUNT(*) AS num_rows,
    CORR(shipping_fee, discount) AS corr_shipping_fee_discount,
    SIZE(COLLECT_SET(status)) AS num_status
FROM orders
GROUP BY CASE WHEN amount >= 1520 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY corr_shipping_fee_discount;
