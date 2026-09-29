SELECT AVG(amount) AS avg_amount FROM transactions WHERE merchant IN ('Grab', 'Lazada') ORDER BY avg_amount;
