SELECT is_flagged, MIN(amount) AS min_amount FROM transactions WHERE account_id IN (17, 33) GROUP BY is_flagged ORDER BY min_amount DESC;
