SELECT is_flagged, total_amount FROM (SELECT is_flagged, SUM(amount) AS total_amount FROM transactions GROUP BY is_flagged) AS agg WHERE total_amount < 21000
