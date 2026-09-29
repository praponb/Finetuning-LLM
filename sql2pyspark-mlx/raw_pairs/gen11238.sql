SELECT id, country, credit_limit, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) credit_limit_row FROM customers ORDER BY country;
