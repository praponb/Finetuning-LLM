SELECT city, credit_limit_range FROM (SELECT city, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range FROM customers GROUP BY city) AS t WHERE credit_limit_range > 56;
