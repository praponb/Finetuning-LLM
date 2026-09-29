SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rnk FROM subscriptions WHERE start_date BETWEEN '2024-12-01' AND '2025-08-28') AS t WHERE rnk = 1
