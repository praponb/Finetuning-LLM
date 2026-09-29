SELECT status, ROUND(AVG(amount), 2) avg_amount FROM payments WHERE amount >= 480 AND amount <= 550 GROUP BY status ORDER BY status ASC, avg_amount ASC;
