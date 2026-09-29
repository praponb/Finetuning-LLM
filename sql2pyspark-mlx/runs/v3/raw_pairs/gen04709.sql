SELECT account_id, amazon_count
FROM (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) AS amazon_count
    FROM transactions
    GROUP BY account_id
) AS sub
WHERE amazon_count >= 79;
