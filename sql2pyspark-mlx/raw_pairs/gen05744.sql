SELECT id, plan, end_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rnk FROM subscriptions) AS t WHERE rnk <= 2;
