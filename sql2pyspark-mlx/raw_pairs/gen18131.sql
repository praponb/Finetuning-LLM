SELECT *
FROM (
    SELECT CASE WHEN merchant IN ('7-Eleven', 'Grab') THEN 'priority' ELSE 'other' END AS merchant_group, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY CASE WHEN merchant IN ('7-Eleven', 'Grab') THEN 'priority' ELSE 'other' END
) AS s
WHERE avg_amount < 1400;
