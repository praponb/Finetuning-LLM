SELECT * FROM (SELECT is_flagged, merchant, amount FROM transactions)
PIVOT (SUM(amount) FOR merchant IN ('Starbucks', 'Grab', 'Lazada'))
ORDER BY is_flagged;
