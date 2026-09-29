SELECT is_flagged, SUM(amount) total_amount, SUM(SUM(amount)) OVER (ORDER BY is_flagged) AS cumulative_amount FROM transactions GROUP BY is_flagged ORDER BY cumulative_amount, is_flagged LIMIT 1
