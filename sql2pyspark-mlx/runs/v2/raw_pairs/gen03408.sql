SELECT *
FROM (
    SELECT is_active, MIN(credit_limit) AS min_credit_limit, MAX(credit_limit) AS max_credit_limit
    FROM customers
    GROUP BY is_active
) AS agg
WHERE max_credit_limit >= 5700
ORDER BY max_credit_limit DESC, min_credit_limit DESC;
