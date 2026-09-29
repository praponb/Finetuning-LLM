SELECT id, opened_date, RANK() OVER (ORDER BY opened_date) AS rnk
FROM stores;
