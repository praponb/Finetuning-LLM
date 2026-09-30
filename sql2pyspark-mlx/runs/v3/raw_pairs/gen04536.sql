SELECT *
FROM (
    SELECT method, MAX(amount) AS max_amount, SORT_ARRAY(COLLECT_SET(method)) AS method_list
    FROM payments
    WHERE method IS NULL
    GROUP BY method
) AS s
WHERE max_amount > 840;
