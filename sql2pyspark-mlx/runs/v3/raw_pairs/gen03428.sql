SELECT is_flagged, MAX(amount) AS max_amount FROM transactions GROUP BY is_flagged ORDER BY max_amount DESC;
