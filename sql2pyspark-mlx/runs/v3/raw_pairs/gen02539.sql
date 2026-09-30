SELECT is_flagged, merchant, COUNT(*) AS total_count, COUNT(amount) AS amount_count FROM transactions GROUP BY is_flagged, merchant ORDER BY total_count;
