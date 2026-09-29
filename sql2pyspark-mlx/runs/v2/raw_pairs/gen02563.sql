SELECT status, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE method = 'promptpay' AND method IS NULL
GROUP BY status
ORDER BY stddev_amount DESC
