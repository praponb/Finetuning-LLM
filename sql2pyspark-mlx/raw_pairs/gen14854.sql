SELECT * FROM (SELECT merchant, MIN(amount) AS min_amount FROM transactions GROUP BY merchant) AS sub WHERE min_amount >= 1300;
