SELECT is_flagged, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM transactions GROUP BY is_flagged ORDER BY amount_rank ASC;
