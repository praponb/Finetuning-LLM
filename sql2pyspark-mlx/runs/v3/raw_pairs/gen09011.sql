SELECT txn_type_group, avg_amount
FROM (
    SELECT CASE WHEN txn_type IN ('CREDIT', 'REFUND') THEN 'group_a' ELSE 'other' END AS txn_type_group, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE merchant IN ('Starbucks', '7-Eleven', 'Shell')
    GROUP BY CASE WHEN txn_type IN ('CREDIT', 'REFUND') THEN 'group_a' ELSE 'other' END
) AS sub
WHERE avg_amount >= 48
