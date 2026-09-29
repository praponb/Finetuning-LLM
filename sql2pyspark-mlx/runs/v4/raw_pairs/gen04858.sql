SELECT * FROM customers QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit ASC, id ASC) = 1;
