SELECT COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count, MAX(amount) AS max_amount FROM transactions ORDER BY max_amount DESC;
