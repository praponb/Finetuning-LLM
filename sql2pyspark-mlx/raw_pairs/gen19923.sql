SELECT is_flagged, MAX(amount) - MIN(amount) AS amount_range, SUM(amount) AS total_amount FROM transactions GROUP BY is_flagged;
