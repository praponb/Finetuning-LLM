SELECT method, AVG(amount) AS avg_amount FROM payments WHERE method <> 'card' GROUP BY method;
