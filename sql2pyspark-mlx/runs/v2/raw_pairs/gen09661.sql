SELECT is_flagged, SUM(amount) AS total_amount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions GROUP BY is_flagged;
