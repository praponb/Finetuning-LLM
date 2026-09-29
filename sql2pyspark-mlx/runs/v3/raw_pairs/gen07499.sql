SELECT status, method_list, stddev_amount
FROM (
    SELECT status, SORT_ARRAY(COLLECT_SET(method)) AS method_list, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM payments
    WHERE paid_at >= '2023-09-24'
    GROUP BY status
) AS agg
WHERE stddev_amount > 21
ORDER BY stddev_amount DESC;
