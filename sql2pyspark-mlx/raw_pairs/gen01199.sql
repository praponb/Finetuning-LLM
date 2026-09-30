SELECT status, CASE WHEN method IN ('bank_transfer', 'card') THEN 'group_a' ELSE 'other' END AS method_group, COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count
FROM payments
GROUP BY 1, 2
