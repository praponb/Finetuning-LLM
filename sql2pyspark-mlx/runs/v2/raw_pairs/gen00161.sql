SELECT is_flagged, MIN(amount) AS min_amount FROM transactions WHERE is_flagged GROUP BY is_flagged HAVING MIN(amount) > 3200 ORDER BY 2;
