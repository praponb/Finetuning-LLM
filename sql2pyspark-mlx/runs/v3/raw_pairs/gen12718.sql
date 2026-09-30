SELECT *
FROM (
    SELECT CASE WHEN amount >= 1460 THEN 'high' WHEN amount >= 230 THEN 'mid' ELSE 'low' END AS amount_bucket, SORT_ARRAY(COLLECT_SET(method)) AS method_list, COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count
    FROM payments
    WHERE order_id IN (1, 2, 5, 12)
    GROUP BY CASE WHEN amount >= 1460 THEN 'high' WHEN amount >= 230 THEN 'mid' ELSE 'low' END
) AS t
WHERE cash_count > 8
ORDER BY 3 DESC, amount_bucket
LIMIT 3;
