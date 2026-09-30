SELECT paid_quarter, success_count, avg_amount
FROM (
    SELECT QUARTER(paid_at) AS paid_quarter, COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY QUARTER(paid_at)
) AS x
WHERE avg_amount >= 51
ORDER BY paid_quarter DESC;
