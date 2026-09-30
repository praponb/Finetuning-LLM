SELECT * FROM (SELECT city, segment, country, credit_limit FROM customers) src PIVOT (MAX(credit_limit) AS amount FOR country IN ('US', 'SG'));
